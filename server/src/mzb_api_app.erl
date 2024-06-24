-module(mzb_api_app).
-behaviour(application).

%% API.
-export([start/2, prep_stop/1, stop/1, default_logger/0, load_config/1]).

%% API.

start(_Type, _Args) ->
    ok = load_config(mzbench_api),
    ok = load_cloud_plugin(),

    {ok, Sup} = mzb_api_sup:start_link(),
    start_http_server(),
    
    {ok, Sup, #{}}.

start_http_server() ->
    Dispatch = cowboy_router:compile([
        {'_', [
            {"/",                        cowboy_static, {priv_file, mzbench_api, "/http_root/index.html"}},
            {"/[:vsn]/dev",              cowboy_static, {priv_file, mzbench_api, "/http_root/index.dev.html"}},
            {"/[:vsn]/js/vendors/[...]", cowboy_static, {priv_dir, mzbench_api, ["/http_root/js/vendors"], [{mimetypes, cow_mimetypes, web}]}},
            {"/[:vsn]/js/[...]",         cowboy_static, {priv_dir, mzbench_api, ["/http_root/js"], [{mimetypes, cow_mimetypes, web}]}},
            {"/[:vsn]/css/[...]",        cowboy_static, {priv_dir, mzbench_api, ["/http_root/css"], [{mimetypes, cow_mimetypes, web}]}},
            {"/[:vsn]/favicon.ico",      cowboy_static, {priv_file, mzbench_api, "/http_root/favicon.ico"}},
            {"/[:vsn]/ws",               mzb_api_ws_handler, []},
            {'_',                        mzb_api_endpoints, []}
        ]}
    ]),
    {ok, CowboyInterfaceStr} = application:get_env(mzbench_api, network_interface),
    {ok, CowboyInterface} = inet_parse:address(CowboyInterfaceStr),
    {ok, CowboyPort} = application:get_env(mzbench_api, listen_port),
    {ok, Protocol} =  application:get_env(mzbench_api, protocol),
    lager:info("Starting cowboy ~p listener on ~p:~p", [Protocol, CowboyInterface, CowboyPort]),
    Params = [{port, CowboyPort}, {ip, CowboyInterface}],
    Env = [{env, [{dispatch, Dispatch}]}],
    {ok, _} = case Protocol of
        http -> cowboy:start_http(http, 100, Params, Env);
        https ->
            {ok, CertFile} = application:get_env(mzbench_api, certfile),
            {ok, KeyFile} = application:get_env(mzbench_api, keyfile),
            CACertInList =  case application:get_env(mzbench_api, certfile, none) of
                                none -> [];
                                F -> [{cacertfile, mzb_file:expand_filename(F)}]
                            end,
            cowboy:start_https(https, 100, Params ++ CACertInList
                                ++ [{certfile, mzb_file:expand_filename(CertFile)},
                                    {keyfile, mzb_file:expand_filename(KeyFile)}], Env)
        end,
    ok.

prep_stop(State) ->
    lager:warning("Server is going to shutdown!"),
    mzb_api_firehose:notify(danger, "Server is going to shutdown!"),
    %% deactivate stops all benchmarks. we are waiting 120 secs 
    %% to be sure that benchmark's finalize are finished
    mzb_api_server:deactivate(),
    wait_benchmarks_finish(_AttemptNum = 120),
    State.

stop(_State) ->
    lager:warning("Server is stopping..."),
    ok = cowboy:stop_listener(http),
    ok.

wait_benchmarks_finish(Attempts) when Attempts =< 0 -> ok;
wait_benchmarks_finish(Attempts) ->
    Benchmarks = supervisor:which_children(benchmarks_sup),
    BenchmarksNum = length(Benchmarks),
    case BenchmarksNum > 0 of
        true  ->
            lager:info("Waiting for: ~p", [Benchmarks]),
            timer:sleep(1000),
            wait_benchmarks_finish(Attempts - 1);
        false ->
            lager:info("All benchmarks finished"),
            ok
    end.

load_config(AppName) ->
    case os:getenv("MZBENCH_CONFIG_FILE") of
        false ->
            {ok, Configs} = application:get_env(mzbench_api, server_configs),
            _ = lists:dropwhile(
                fun (Cfg) ->
                    try
                        load_config(Cfg, AppName),
                        ok = application:set_env(mzbench_api, active_config, Cfg),
                        false
                    catch
                        _:{config_read_error, _, enoent} -> true
                    end
                end, Configs),
            ok;
        Config ->
            ok = application:set_env(mzbench_api, active_config, Config),
            load_config(Config, AppName)
    end.

load_config(File, AppName) ->
    case file:consult(mzb_file:expand_filename(File)) of
        {ok, [Config]} ->
            lists:foreach(fun ({App, Env}) when App == AppName ->
                                lager:info("Reading configuration from ~ts for ~ts~n~p", [File, AppName, Env]),
                                [ application:set_env(App, Key, Val) || {Key, Val} <- Env];
                              (_) -> ok
                          end, Config),
            ok;
        {error, Reason} ->
            erlang:error({config_read_error, File, Reason})
    end.

load_cloud_plugin() ->
    Dir = mzb_api_paths:plugins_dir(),
    ok = filelib:ensure_dir(filename:join(Dir, ".")),
    PluginPaths = mzb_file:wildcard(filename:join([Dir, "*", "ebin"])),
    ok = code:add_pathsa(PluginPaths),
    lager:info("PATHS: ~p", [code:get_path()]),
    ok.

% We can't call lager:Severity(...) because lager uses parse_transform
default_logger() ->
    fun (debug, F, A) -> lager:debug(F, A);
        (info, F, A) -> lager:info(F, A);
        (error, F, A) -> lager:error(F, A)
    end.

