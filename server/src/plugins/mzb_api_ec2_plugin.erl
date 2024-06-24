-module(mzb_api_ec2_plugin).

-export([start/2, create_cluster/3, destroy_cluster/1]).

-include_lib("erlcloud/include/erlcloud.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("erlcloud/include/erlcloud_ec2.hrl").

-define(POLL_INTERVAL, 2000).
-define(MAX_POLL_COUNT, 150).

% ===========================================================
% Public API
% ===========================================================

start(_Name, Opts) ->
    Opts.

% couldn't satisfy both erl 18 and 19 dialyzers, spec commented
%-spec create_cluster(#{config:=[any()], instance_spec:=[any()], instance_user:=_, _=>_}, NumNodes :: pos_integer(), Config :: #{}) -> {ok, {map(),[any()]}, string(), [string()]}.
create_cluster(Opts = #{instance_user:= UserName}, NumNodes, Config) when is_integer(NumNodes), NumNodes > 0 ->
    Overhead = mzb_bc:maps_get(overhead, Opts, 0),
    {ok, Data} = erlcloud_ec2:run_instances(instance_spec(NumNodes, Overhead, Opts), get_config(Opts)),
    Instances = proplists:get_value(instances_set, Data),
    Ids = [proplists:get_value(instance_id, X) || X <- Instances],
    try
        {ok, _} = erlcloud_ec2:create_tags(Ids, [{"Name", maps:get(purpose, Config, "")}], get_config(Opts)),
        StartedIds = wait_nodes_start(NumNodes, Ids, Opts, ?MAX_POLL_COUNT),
        {ok, [NewData]} = get_description(StartedIds, Opts, ?MAX_POLL_COUNT),
        lager:info("~p", [NewData]),
        IPs = get_IPs(StartedIds, NewData),
        wait_nodes_ssh(IPs, ?MAX_POLL_COUNT),
        {ok, {Opts, Ids}, UserName, IPs}
    catch
        C:E:ST ->
            destroy_cluster({Opts, Ids}),
            erlang:raise(C,E,ST)
    end.

get_description(_, _, C) when C < 0 -> {ec2_error, cluster_getinfo_timeout};
get_description(Ids, Opts, C) ->
    case erlcloud_ec2:describe_instances(Ids, get_config(Opts)) of
        {ok, _} = Data -> Data;
        _ -> timer:sleep(?POLL_INTERVAL),
             get_description(Ids, Opts, C - 1)
    end.

% try to get ip addresses for allocated hosts
-spec get_IPs([string()], [any(), ...]) -> [string(), ...].
get_IPs(Ids, Data) ->
    get_IPs(Ids, Data, [ip_address, private_ip_address]).

-spec get_IPs([string()], [any(), ...], [atom()]) -> [string()].
get_IPs(_, _, []) -> erlang:error({ec2_error, couldnt_obtain_hosts});
get_IPs(Ids, Data, [H | T]) ->
    Instances = proplists:get_value(instances_set, Data),
    IPs = [proplists:get_value(H, X) || X <- Instances],
    case IPs of
        [R | _] when R =/= undefined, R =/= "" -> IPs;
        _ -> get_IPs(Ids, Data, T)
    end.

% couldn't satisfy both erl 18 and 19 dialyzers, spec commented
%-spec destroy_cluster({#{config:=[any()], _=>_}, [term()]}) -> ok.

destroy_cluster({Opts, Ids}) ->
    R = erlcloud_ec2:terminate_instances(Ids, get_config(Opts)),
    lager:info("Deallocating ids: ~p, result: ~p", [Ids, R]),
    {ok, _} = R,
    ok.

wait_nodes_ssh(_, C) when C < 0 -> erlang:error({ec2_error, cluster_ssh_start_timed_out});
wait_nodes_ssh([], _) -> ok;
wait_nodes_ssh([H | T], C) ->
    lager:info("Checking port 22 on ~p", [H]),
    R = gen_tcp:connect(H, 22, [], ?POLL_INTERVAL),
    case R of
        {ok, Socket} -> gen_tcp:close(Socket), wait_nodes_ssh(T, C);
        _ -> timer:sleep(?POLL_INTERVAL), wait_nodes_ssh([H | T], C - 1)
    end.

wait_nodes_start(_, _, _, C) when C < 0 -> erlang:error({ec2_error, cluster_start_timed_out});
wait_nodes_start(0, _, _, _) -> [];
wait_nodes_start(N, [H | T], Opts, C) ->
    {ok, Res} = erlcloud_ec2:describe_instance_status([{"InstanceId", H}], [], get_config(Opts)),
    lager:info("Waiting nodes result: ~p", [Res]),
    Status = case Res of
        [P | _] -> proplists:get_value(instance_state_name, P);
             _  -> undefined
    end,
    case Status of
        "running"  -> [H | wait_nodes_start(N - 1, T, Opts, C - 1)];
        _ -> timer:sleep(?POLL_INTERVAL),
             wait_nodes_start(N, T ++ [H], Opts, C - 1)
    end.

instance_spec(NumNodes, Overhead, #{instance_spec:= Ec2AppConfig}) ->
    lists:foldr(fun({Name, Value}, A) -> set_record_element(A, Name, Value) end,
        #ec2_instance_spec{
            min_count = NumNodes,
            max_count = trunc(NumNodes*(1 + Overhead / 100)),
            iam_instance_profile_name = "undefined"},
        Ec2AppConfig).

get_config(#{config:= AppConfig}) ->
    lists:foldr(fun({Name, Value}, A) -> set_record_element(A, Name, Value) end,
        #aws_config{}, AppConfig).

% Following three functions no more than just a record field setters
set_record_element(Record, Field, Value) ->
    RecordName = erlang:element(1, Record),
    setelement(record_field_num(RecordName, Field), Record, Value).

record_field_num(Record, Field) ->
    Fields = record_fields(Record),
    case length(lists:takewhile(fun (F) -> F /= Field end, Fields)) of
        Length when Length =:= length(Fields) ->
            erlang:error({bad_record_field, Record, Field});
        Length -> Length + 2
    end.

record_fields(ec2_instance_spec) -> record_info(fields, ec2_instance_spec);
record_fields(aws_config) -> record_info(fields, aws_config).
