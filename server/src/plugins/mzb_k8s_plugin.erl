-module(mzb_k8s_plugin).

-export([start/2, create_cluster/3, destroy_cluster/1]).

-define(POLL_INTERVAL, 1000).
-define(MAX_POLL_COUNT, 150).

% ===========================================================
% Public API
% ===========================================================

% Possible config: 
% {cloud_plugins,
%    [{k8s, #{module => mzb_k8s_plugin,
%             context => "stage.k8s", % optional, will be used from k8s
%             namespace => "testing-and-quality", % optional, will be used from k8s
%             pod_user => "bench", % optional, default: root
%             pod_spec => [
%               {image, "http://docker.io/ridrisov/mzbench:mylabel3},
%               {cpu, "1"},       % limits = request
%               {memory, "128Mi"},
%             ]}
%     }
%    ]}

-type plugin_opts() :: #{pod_spec:=[any()], pod_user:=string(), namespace:=string(), context:=string(), _=>_}.
-type cluster_id() :: {Namespace :: string(), RCName :: string()}.

-spec start(any(), plugin_opts()) -> any().
start(_Name, PluginOpts) ->
    PluginOpts.


-spec create_cluster(plugin_opts(), NumNodes :: pos_integer(), ClusterConfig :: #{}) -> {ok, cluster_id(), string(), [string()]}.
create_cluster(PluginOpts, NumNodes, ClusterConfig) when is_integer(NumNodes), NumNodes > 0 ->
    UserName = maps:get(pod_user, PluginOpts, root),
    Namespace = maps:get(namespace, PluginOpts, undefined),
    Context = maps:get(context, PluginOpts, undefined),
    PodSpec = maps:get(pod_spec, PluginOpts),

    BenchId = maps:get(bench_id, ClusterConfig),
    Image = case maps:get(node_image, ClusterConfig) of
                undefined -> get_config_value(image, PodSpec);
                V -> V
            end,
    BenchName = "mzbench-" ++ integer_to_list(BenchId),
    ID = {Context, Namespace, BenchName},


    try
        {ok, _} = create_rc(ID, Image, PodSpec, NumNodes),
        {ok, PodNames} = wait_pods_creation(Context, Namespace, BenchName, NumNodes, ?MAX_POLL_COUNT),
        lager:info("Pods are created: ~p", [PodNames]),
        wait_pods_start(NumNodes, ID, PodNames, ?MAX_POLL_COUNT),
        lager:info("Pods are running ~p", [PodNames]),
        % IP addresses were not known before
        {ok, PodData2} = get_pods(Context, Namespace, ["-l bench=" ++ BenchName]),
        IPs = get_pod_ips(PodData2),
        wait_pods_ssh(IPs, ?MAX_POLL_COUNT),
        lager:info("Pods are ssh-ready ~p", [PodNames]),
        {ok, ID, UserName, IPs}
    catch
        C:E:ST ->
            destroy_cluster(ID),
            erlang:raise(C,E,ST)
    end.

destroy_cluster(ID) ->
    R = delete_rc(ID),
    lager:info("Destroyed RC: ~p, result: ~p", [ID, R]),
    {ok, _} = R,
    ok.


%%%%%%%%%%%%%%%%%%%% 
% Internal API
%%%%%%%%%%%%%%%%%%%% 

context_arg(undefined) -> [];
context_arg(Context) -> ["--context", Context].

namespace_arg(undefined) -> [];
namespace_arg(Namespace) -> ["--namespace", Namespace].

create_rc({Context, Namespace, BenchName}, Image, PodSpec, NumNodes) ->
    % Might be a good idea to be able to define these values in the benchmark
    CPU = get_config_value(cpu, PodSpec),
    Memory = get_config_value(memory, PodSpec),

    Resources = "cpu=" ++ CPU ++ ",memory=" ++ Memory,
    Cmd = ["kubectl", "run", BenchName] ++
          context_arg(Context) ++
          namespace_arg(Namespace) ++
          ["--labels", "app=mzbench-worker,bench=" ++ BenchName,
           "--image", Image,
           "--replicas", integer_to_list(NumNodes),
           "--requests", Resources,
           "--limits", Resources,
           "--port", "22",
           "--generator", "run/v1",
           "--", "/usr/sbin/sshd", "-D"],
    run_kubectl(Cmd, false).

delete_rc({Context, Namespace, BenchName}) ->
    Cmd = ["kubectl", "delete", "rc", BenchName] ++
          context_arg(Context) ++
          namespace_arg(Namespace),
    run_kubectl(Cmd, false).

get_pods(Context, Namespace, Args) ->
    Cmd = ["kubectl -o json get pods"] ++
          context_arg(Context) ++
          namespace_arg(Namespace) ++
          Args,
    run_kubectl(Cmd, true).

-spec run_kubectl([any()], boolean()) -> {ok, Res :: any()} | {error, Why :: string()}.
run_kubectl(CmdList, ParseJson) ->
    Logger = mzb_api_app:default_logger(),
    Cmd = string:join(CmdList, " "),
    case mzb_subprocess:check_output(Cmd, [], [], Logger) of
        {0, Res} when ParseJson -> {ok, jiffy:decode(Res, [return_maps])};
        {0, Res} -> {ok, Res};
        {_Code, Res} -> {error, Res}
    end.


get_pod_names(PodData) ->
    [ get_map_value(<<"metadata">>, <<"name">>, Pod) || Pod <- maps:get(<<"items">>, PodData) ].

get_pod_ips(PodData) ->
    [ get_map_value(<<"status">>, <<"podIP">>, Pod) || Pod <- maps:get(<<"items">>, PodData) ].

% wait_pods_start will fail if len(Pods) < NumNodes
wait_pods_start(_, _, _, C) when C < 0 -> erlang:error({k8s_error, cluster_start_timed_out});
wait_pods_start(0, _, _, _) -> [];
wait_pods_start(N, ID, [H | T], C) ->
    {Context, Namespace, _} = ID,
    {ok, Res} = get_pods(Context, Namespace, [H]),
    lager:info("Waiting pods start: ~p", [Res]),
    case get_map_value(<<"status">>, <<"phase">>, Res) of
        "Running" ->
            [H | wait_pods_start(N-1, ID, T, C - 1)];
        _ ->
            timer:sleep(?POLL_INTERVAL),
            wait_pods_start(N, ID, T ++ [H], C - 1)
    end.

wait_pods_ssh(_, C) when C < 0 -> erlang:error({k8s_error, cluster_ssh_start_timed_out});
wait_pods_ssh([], _) -> ok;
wait_pods_ssh([H | T], C) ->
    lager:info("Checking port 22 on ~p", [H]),
    case gen_tcp:connect(H, 22, [], ?POLL_INTERVAL) of
        {ok, Socket} -> gen_tcp:close(Socket), wait_pods_ssh(T, C - 1);
        _ -> timer:sleep(?POLL_INTERVAL), wait_pods_ssh([H | T], C - 1)
    end.

wait_pods_creation(_, _, _, _, C) when C < 0 -> erlang:error({k8s_error, claster_creation_timed_out});
wait_pods_creation(Context, Namespace, BenchName, NumNodes, C) ->
    {ok, PodData1} = get_pods(Context, Namespace, ["-l bench=" ++ BenchName]),
    PodNames = get_pod_names(PodData1),
    case length(PodNames) == NumNodes of
        true -> {ok, PodNames};
        _ ->
            timer:sleep(?POLL_INTERVAL),
            wait_pods_creation(Context, Namespace, BenchName, NumNodes, C-1)
    end.

get_map_value(Key1, Key2, Map) ->
    V = maps:get(Key2, maps:get(Key1, Map)),
    binary_to_list(V).

get_config_value(Key, PropList) ->
    Value = proplists:get_value(Key, PropList),
    case Value == undefined orelse not is_list(Value) of
        true -> erlang:error({k8s_error, incorrect_config_value, Key});
        _ -> Value
    end.
