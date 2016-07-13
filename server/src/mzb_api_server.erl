-module(mzb_api_server).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    deactivate/0,
    start_bench/1,
    restart_bench/1,
    stop_bench/1,
    change_env/2,
    bench_foldl/2,
    get_info/5,
    bench_finished/2,
    status/1,
    server_data_dir/0,
    ensure_started/0,
    email_report/2,
    is_datastream_ended/1,
    add_tags/2,
    remove_tags/2
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

deactivate() ->
    gen_server:call(?MODULE, deactivate, infinity).

ensure_started() ->
    case gen_server:call(?MODULE, is_ready) of
        true -> ok;
        false -> erlang:error(server_not_active)
    end.

start_bench(Params) ->
    case gen_server:call(?MODULE, {start_bench, Params}, infinity) of
        {ok, Resp} -> Resp;
        {error, {exception, {C,E,ST}}} -> erlang:raise(C,E,ST);
        {error, Reason} -> erlang:error(Reason)
    end.

restart_bench(Id) ->
    case gen_server:call(?MODULE, {restart_bench, Id}, infinity) of
        {ok, Resp} -> Resp;
        {error, {exception, {C,E,ST}}} -> erlang:raise(C,E,ST);
        {error, not_found} ->
            erlang:error({not_found, io_lib:format("Benchmark ~p is not found", [Id])});
        {error, Reason} -> erlang:error(Reason)
    end.

stop_bench(Id) ->
    case gen_server:call(?MODULE, {stop_bench, Id}, infinity) of
        ok -> ok;
        {error, not_found} ->
            erlang:error({not_found, io_lib:format("Benchmark ~p is not found", [Id])})
    end.

change_env(Id, Env) ->
    case ets:lookup(benchmarks, Id) of
        [{_, B, undefined}] ->
            mzb_api_bench:change_env(B, Env);
        [{_, _, _}] ->
            erlang:error({badarg, io_lib:format("Benchmark ~p is finished", [Id])});
        [] ->
            erlang:error({not_found, io_lib:format("Benchmark ~p is not found", [Id])})
    end.

status(Id) ->
    case ets:lookup(benchmarks, Id) of
        [{_, B, undefined}] when is_pid(B) ->
            mzb_api_bench:get_status(B);
        [{_, _, Status}] ->
            Status;
        [] ->
            erlang:error({not_found, io_lib:format("Benchmark ~p is not found", [Id])})
    end.

bench_finished(Id, Status) ->
    gen_server:cast(?MODULE, {bench_finished, Id, Status}).

is_datastream_ended(Id) ->
    case ets:lookup(benchmarks, Id) of
        [{_, B, _}] when is_pid(B) -> false;
        [{_, _, _Status}] -> true;
        [] -> erlang:error({not_found, io_lib:format("Benchmark ~p is not found", [Id])})
    end.

add_tags(Id, Tags) ->
    Res =
        case ets:lookup(benchmarks, Id) of
            [{_, B, undefined}] when is_pid(B) ->
                try
                    mzb_api_bench:add_tags(B, Tags)
                catch
                    exit:{no_proc, _} -> gen_server:call(?MODULE, {add_tags, Id, Tags})
                end;
            _ ->
                gen_server:call(?MODULE, {add_tags, Id, Tags})
        end,
    case Res of
        ok ->
            mzb_api_firehose:update_bench(mzb_api_server:status(Id)),
            ok;
        {error, not_found} -> erlang:error({not_found, io_lib:format("Benchmark ~p is not found", [Id])});
        {error, invalid_benchmark} -> erlang:error({invalid_benchmark, io_lib:format("Benchmark ~p is in invalid state", [Id])})
    end.

remove_tags(Id, Tags) ->
    Res =
        case ets:lookup(benchmarks, Id) of
            [{_, B, undefined}] when is_pid(B) ->
                try
                    mzb_api_bench:remove_tags(B, Tags)
                catch
                    exit:{no_proc, _} -> gen_server:call(?MODULE, {remove_tags, Id, Tags})
                end;
            _ ->
                gen_server:call(?MODULE, {remove_tags, Id, Tags})
        end,
    case Res of
        ok ->
            mzb_api_firehose:update_bench(mzb_api_server:status(Id)),
            ok;
        {error, not_found} -> erlang:error({not_found, io_lib:format("Benchmark ~p is not found", [Id])});
        {error, invalid_benchmark} -> erlang:error({invalid_benchmark, io_lib:format("Benchmark ~p is in invalid state", [Id])})
    end.

bench_foldl(Fun, AccInit) ->
    ets:foldl(
        fun ({Id, Pid, undefined}, Acc) ->
                Fun(Id, mzb_api_bench:get_status(Pid), Acc);
            ({Id, _, Status}, Acc) ->
                Fun(Id, Status, Acc)
        end, AccInit, benchmarks).

get_info(Filter, undefined, undefined, undefined, Limit) ->
    {Res, Min, _} = getprev(ets:last(benchmarks), Filter, Limit, []),
    {Res, Min, undefined};
get_info(Filter, Max, undefined, undefined, Limit) when is_integer(Max) ->
    getprev(ets:prev(benchmarks, Max), Filter, Limit, []);
get_info(Filter, undefined, Id, undefined, Limit) when is_integer(Id) ->
    {Res, _Min, Max} = getnext(Id, Filter, Limit, []),
    L = length(Res),
    if
        L < Limit ->
            {Res2, Min2, _Max2} = getprev(ets:prev(benchmarks, Id), Filter, Limit - L, []),
            {Res ++ Res2, Min2, Max};
        true ->
            {Res2, Min2, Max2} = getprev(ets:prev(benchmarks, Id), Filter, Limit, []),
            L2 = length(Res2),
            if
                L2 < Limit ->
                    {Res3, _Min3, Max3} = getnext(ets:next(benchmarks, Id), Filter, Limit - L2, []),
                    {Res3 ++ Res2, Min2, Max3};
                true ->
                    {Res2, Min2, Max2}
            end
    end;
get_info(Filter, undefined, undefined, Min, Limit) when is_integer(Min) ->
    {Res1, Min1, Max1} = getnext(ets:next(benchmarks, Min), Filter, Limit, []),
    L = length(Res1),
    if
        L < Limit ->
            {Res2, Min2, _} = getprev(Min, Filter, Limit - L, []),
            {Res1 ++ Res2, Min2, Max1};
        true ->
            {Res1, Min1, Max1}
    end.

getprev('$end_of_table', _, _Limit, Acc) ->
    Res = lists:reverse(Acc),
    {_Min, Max} = get_boundaries(Res),
    {Res, undefined, Max};
getprev(_, _, Limit, Acc) when length(Acc) >= Limit ->
    Res = lists:reverse(Acc),
    {Min, Max} = get_boundaries(Res),
    {Res, Min, Max};
getprev(Id, Filter, Limit, Acc) ->
    try Filter({Id, status(Id)}) of
        [] -> getprev(ets:prev(benchmarks, Id), Filter, Limit, Acc);
        [S] -> getprev(ets:prev(benchmarks, Id), Filter, Limit, [S|Acc])
    catch
        error:{not_found, _} ->
            Res = lists:reverse(Acc),
            {Min, Max} = get_boundaries(Res),
            {Res, Min, Max}
    end.

getnext('$end_of_table', _, _Limit, Acc) ->
    {Min, _Max} = get_boundaries(Acc),
    {Acc, Min, undefined};
getnext(_, _, Limit, Acc) when length(Acc) >= Limit ->
    {Min, Max} = get_boundaries(Acc),
    {Acc, Min, Max};
getnext(Id, Filter, Limit, Acc) ->
    try Filter({Id, status(Id)}) of
        [] -> getnext(ets:next(benchmarks, Id), Filter, Limit, Acc);
        [S] -> getnext(ets:next(benchmarks, Id), Filter, Limit, [S|Acc])
    catch
        error:{not_found, _} ->
            {Min, Max} = get_boundaries(Acc),
            {Acc, Min, Max}
    end.

get_boundaries([]) -> {undefined, undefined};
get_boundaries([#{id:= Max}|_] = L) ->
    [#{id:= Min}|_] = lists:reverse(L),
    {Min, Max}.


email_report(Id, Emails) ->
    lager:info("[ SERVER ] Report req for #~b for emails: ~p", [Id, Emails]),
    Status = case ets:lookup(benchmarks, Id) of
        [{_, _BenchPid, undefined}] ->
            erlang:error({badarg, "Benchmark in not ended yet"});
        [{_, _, S}] -> S;
        [] ->
            erlang:error({not_found, io_lib:format("Benchmark ~p is not found", [Id])})
    end,
    case mzb_api_bench:send_email_report(Emails, Status) of
        ok -> ok;
        {error, {Error, Stacktrace}} ->
            lager:error("Send report to ~p failed with reason: ~p~n~p",
                        [Emails, Error, Stacktrace]),
            erlang:error(Error)
    end.

init([]) ->
    _ = ets:new(benchmarks, [named_table, ordered_set, protected]),
    ServerDir = server_data_dir(),
    ok = filelib:ensure_dir(filename:join(ServerDir, ".")),
    MaxId = import_data(ServerDir),
    User = sys_username(),
    {ok, MaxBenchNum} = application:get_env(mzbench_api, max_bench_num),
    {ok, _} = dets:open_file(dashboards, [{file, filename:join(ServerDir, "dashboards")}, {type, set}]),
    lager:info("Server username: ~p", [User]),

    {ok, check_max_bench_num(#{next_id => MaxId + 1,
           monitors => #{},
           status => active,
           data_dir => ServerDir,
           user => User,
           max_bench_num => MaxBenchNum})}.

server_data_dir() ->
    DataDir = mzb_api_paths:bench_data_dir(),
    filename:absname(DataDir).

handle_call({start_bench, Params}, _From, #{status:= active} = State) ->
    case start_bench_child(Params, State) of
        {ok, Id, NewState} ->
            {reply, {ok, #{id => Id, status => <<"pending">>}}, NewState};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, NewState}
    end;

handle_call({start_bench, Params}, _From, #{status:= inactive} = State) ->
    lager:info("[ SERVER ] Start of bench failed because server is inactive ~p", [Params]),
    {reply, {error, server_inactive}, State};

handle_call({restart_bench, RestartId}, _From, #{status:= active, data_dir:= DataDir} = State) ->
    lager:info("[ SERVER ] Restarting bench #~b", [RestartId]),
    RestartIdStr = erlang:integer_to_list(RestartId),
    ParamsFile = filename:join([DataDir, RestartIdStr, "params.bin"]),
    case file:read_file(ParamsFile) of
        {ok, Binary} ->

            Params =
                % BC code: migration of data, convert dont_provision_nodes to provistion_nodes
                case erlang:binary_to_term(Binary) of
                    #{dont_provision_nodes:= V} = P -> P#{provision_nodes => not V};
                    P -> P
                end,

            case start_bench_child(Params, State) of
                {ok, Id, NewState} ->
                    {reply, {ok, #{id => Id, status => <<"pending">>}}, NewState};
                {error, Reason, NewState} ->
                    {reply, {error, Reason}, NewState}
            end;
        {error, enoent} ->
            {reply, {error, not_found}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({restart_bench, RestartId}, _From, #{status:= inactive} = State) ->
    lager:info("[ SERVER ] Restart of bench failed because server is inactive #~p", [RestartId]),
    {reply, {error, server_inactive}, State};

handle_call(deactivate, _From, #{} = State) ->
    Unfinished = ets:foldl(
        fun ({_, Pid, _}, Acc) when is_pid(Pid) -> [Pid | Acc];
            (_, Acc) -> Acc
        end, [], benchmarks),
    lager:info("[ SERVER ] Stopping all benchmarks due to server stop: ~p", [Unfinished]),
    [ok = mzb_api_bench:interrupt_bench(P) || P <- Unfinished],
    {reply, ok, State#{status:= inactive}};

handle_call({stop_bench, Id}, _, #{} = State) ->
    lager:info("[ SERVER ] Stop bench #~b request received", [Id]),
    case ets:lookup(benchmarks, Id) of
        [{_, BenchPid, undefined}] ->
            ok = mzb_api_bench:interrupt_bench(BenchPid),
            {reply, ok, State};
        [{_, _, _}] ->
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call(is_ready, _, #{status:= active} = State) ->
    {reply, true, State};

handle_call(is_ready, _, #{status:= inactive} = State) ->
    {reply, false, State};

handle_call({add_tags, Id, Tags}, _, State) ->
    case ets:lookup(benchmarks, Id) of
        [{_, _, Status = #{config:= Config}}] ->
            OldTags = mzb_bc:maps_get(tags, Config, []),
            NewTags = OldTags ++ [T || T <- Tags, not lists:member(T, OldTags)],
            NewStatus = maps:put(config, maps:put(tags, NewTags, Config), Status),
            save_results(Id, NewStatus, State),
            {reply, ok, State};
        [{_, _, _}] -> {reply, {error, invalid_benchmark}, State};
        [] -> {reply, {error, not_found}, State}
    end;

handle_call({remove_tags, Id, Tags}, _, State) ->
    case ets:lookup(benchmarks, Id) of
        [{_, _, Status = #{config:= Config}}] ->
            NewTags = mzb_bc:maps_get(tags, Config, []) -- Tags,
            NewStatus = maps:put(config, maps:put(tags, NewTags, Config), Status),
            save_results(Id, NewStatus, State),
            {reply, ok, State};
        [{_, _, _}] -> {reply, {error, invalid_benchmark}, State};
        [] -> {reply, {error, not_found}, State}
    end;

handle_call(_Request, _From, State) ->
    lager:error("Unhandled call: ~p", [_Request]),
    {noreply, State}.

handle_cast({bench_finished, Id, Status}, State) ->
    lager:info("[ SERVER ] Bench #~b finished with status ~p", [Id, maps:get(status, Status)]),
    save_results(Id, Status, State),
    {noreply, State};

handle_cast(_Msg, State) ->
    lager:error("Unhandled cast: ~p", [_Msg]),
    {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, normal}, #{monitors:= Mons} = State) ->
    case maps:find(Ref, Mons) of
        {ok, Id} ->
            true = ets:update_element(benchmarks, Id, {2, undefined}),
            {noreply, State#{monitors => maps:remove(Ref, Mons)}};
        error ->
            lager:error("Received DOWN from unknown process ~p", [Pid]),
            {noreply, State}
    end;

handle_info({'DOWN', Ref, process, Pid, Reason}, #{monitors:= Mons} = State) ->
    case maps:find(Ref, Mons) of
        {ok, Id} ->
            lager:error("Benchmark process #~b ~p has crashed with reason: ~p", [Id, Pid, Reason]),
            true = ets:update_element(benchmarks, Id, {2, undefined}),
            case ets:lookup(benchmarks, Id) of
                [{_, _, undefined}] ->
                    Status = #{status => failed,
                               reason => {crashed, Reason},
                               config => undefined},
                    save_results(Id, Status, State);
                _ -> ok
            end,
            {noreply, State#{monitors => maps:remove(Ref, Mons)}};
        error ->
            lager:error("Received DOWN from unknown process ~p", [Pid]),
            {noreply, State}
    end;

handle_info(_Info, State) ->
    lager:error("Unhandled info: ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_bench_child(Params, #{next_id:= Id, monitors:= Mons, user:= User} = State) ->
    lager:info("[ SERVER ] Start bench #~b", [Id]),
    case supervisor:start_child(benchmarks_sup, [Id, Params#{user => User}]) of
        {ok, Pid} ->
            Mon = erlang:monitor(process, Pid),
            true = ets:insert_new(benchmarks, {Id, Pid, undefined}),
            Status =
                try
                    mzb_api_bench:get_status(Pid)
                catch
                    _:_ ->
                        StartTime = mzb_api_bench:seconds(),
                        #{id => Id, status => zombie, start_time => StartTime, finish_time => StartTime, config => #{}, metrics => #{}}
                end,
            % If server crashes for some reason we want some info about this bench to be saved on disk
            Status2 = Status#{finish_time => maps:get(start_time, Status), status => zombie},
            write_status(Id, Status2, State),
            NewState = State#{next_id => Id + 1, monitors => maps:put(Mon, Id, Mons)},
            {ok, Id, check_max_bench_num(NewState)};
        {error, Reason} ->
            {error, Reason, State#{next_id => Id + 1}}
    end.

check_max_bench_num(#{max_bench_num:= MaxNum, next_id:= NextId, data_dir:= Dir} = State) ->
    MinId = (NextId - MaxNum),
    ets:foldl(
        fun ({_Id, _, undefined}, _) -> ok;
            ({Id, _, _Status}, _) when Id >= MinId -> ok;
            ({Id, _, Status}, _) ->
                case is_favorite(Status) of
                    true ->
                        ok;
                    false ->
                        BenchDir = filename:join(Dir, erlang:integer_to_list(Id)),
                        lager:info("Deleting bench #~b", [Id]),
                        case mzb_file:del_dir(BenchDir) of
                            ok -> ets:delete(benchmarks, Id);
                            {error, Reason} ->
                                lager:error("Delete directory ~p failed: ~p", [BenchDir, Reason])
                        end
                end
        end, [], benchmarks),
    State.

is_favorite(#{config:= Config}) ->
    Tags = mzb_bc:maps_get(tags, Config, []),
    lists:member("favorites", Tags);
is_favorite(_) ->
    false.

save_results(Id, Status, State) ->
    try
        write_status(Id, Status, State),
        true = ets:update_element(benchmarks, Id, {3, Status})
    catch
        _:Error ->
            lager:error("Save bench #~b results failed with reason: ~p~n~p", [Id, Error, erlang:get_stacktrace()])
    end.

write_status(Id, Status, #{data_dir:= Dir}) ->
    Filename = filename:join([Dir, erlang:integer_to_list(Id), "status"]),
    ok = filelib:ensure_dir(Filename),
    ok = file:write_file(Filename, io_lib:format("~p.", [Status])).

import_data(Dir) ->
    lager:info("Importing server data from ~s", [Dir]),

    WC = filename:join(Dir, "*"),

    Items = [D || D <- mzb_file:wildcard(WC), filelib:is_dir(D), [C|_] <- [filename:basename(D)], C /= $.],

    Import = fun (BenchFolder, Max) ->
        File = filename:join([BenchFolder, "status"]),
        try
            IdStr = filename:basename(BenchFolder),
            Id = erlang:list_to_integer(IdStr),
            import_bench_status(Id, File),
            max(Id, Max)
        catch
            _:Error ->
                lager:error("Parsing status filename ~s failed with reason: ~p~n~p", [File, Error, erlang:get_stacktrace()]),
                Max
        end
    end,
    lists:foldl(Import, -1, Items).

import_bench_status(Id, File) ->
    try
        {ok, [Status]} = file:consult(File),
        #{status:= _, start_time := _, finish_time := _, config := #{}} = Status,
        ets:insert(benchmarks, {Id, undefined, Status})
    catch _:E ->
        lager:error("Import from file ~s failed with reason: ~p~n~p", [File, E, erlang:get_stacktrace()])
    end.

sys_username() ->
    Logger = mzb_api_app:default_logger(),
    case os:getenv("REMOTE_USER") of
        false ->
            case os:getenv("USER") of
                false ->
                    try mzb_subprocess:exec_format("who am i | awk '{print $1}'", [], [], Logger) of
                        "" -> mzb_subprocess:exec_format("whoami", [], [], Logger);
                        User -> User
                    catch
                        _:_ -> mzb_subprocess:exec_format("whoami", [], [], Logger)
                    end;
                User -> User
            end;
        User -> User
    end.

