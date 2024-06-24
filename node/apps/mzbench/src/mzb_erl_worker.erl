-module(mzb_erl_worker).

-export([
    add_pathsz/1,
    load/1,
    init/1,
    apply/3,
    apply/4,
    metrics/1,
    terminate/2,
    validate/1,
    validate_function/3]).

validate(Module) ->
    add_pathsz(Module),
    try Module:module_info() of
        _InfoList -> []
    catch
        _:_ ->
            [mzb_string:format("Couldn't get module info for ~p", [Module])]
    end.

add_pathsz(Module) ->
    {ok, WorkerDirs} = application:get_env(mzbench, workers_dirs),

    CodeWildcards =
        [filename:join([D, Module, "ebin"])              || D <- WorkerDirs] ++
        [filename:join([D, Module, "deps", "*", "ebin"]) || D <- WorkerDirs] ++
        [filename:join([D, Module, "_build/default/deps", "*", "ebin"]) || D <- WorkerDirs] ++
        [filename:join([D, Module, "lib" , "*", "ebin"]) || D <- WorkerDirs] ++
        [filename:join([D, Module, "apps", "*", "ebin"]) || D <- WorkerDirs],

    {ok, CurrentDirectory} = file:get_cwd(),
    system_log:info("Add worker wildcards: ~p ~n Current path: ~s", [CodeWildcards, CurrentDirectory]),

    CodePaths = [File || WC <- CodeWildcards, File <- mzb_file:wildcard(WC)],

    system_log:info("Add worker paths: ~p", [CodePaths]),

    code:add_pathsz([filename:absname(P) || P <- CodePaths]).


validate_function(Module, Fn, Arity) ->
    Fns = Module:module_info(exports),
    case lists:member({Fn, Arity + 2}, Fns) of
        true -> ok;
        _ ->
            case lists:member(Fn, [N || {N, _} <- Fns]) of
                true -> bad_arity;
                false -> not_found
            end
    end.

load(Worker) ->
    add_pathsz(Worker),
    WorkerName = case apply_if_exists(Worker, worker_info, []) of
                     {ok, WorkerInfo} ->
                         proplists:get_value(worker_name, WorkerInfo, Worker);
                     {error, not_exists} ->
                         Worker
                 end,
    WorkerStr = atom_to_list(WorkerName),
    WorkerFiles = [filename:join([Dir, WorkerStr, "sys.config"]) ||
                   Dir <- application:get_env(mzbench, workers_dirs, [])],
    {ok, CurrentDir} = file:get_cwd(),
    CurrentDirFiles = [filename:join([CurrentDir, "sys.config"])],
    ok = load_config(WorkerFiles ++ CurrentDirFiles),
    {ok, _} = ensure_all_started(Worker),
    ok.

init(Module) ->
    {Module, Module:initial_state()}.

apply(F, Args, {Module, State}, Meta) ->
    {Result, NewState} = erlang:apply(Module, F, [State, Meta | Args]),
    {Result, {Module, NewState}}.

terminate(Result, {Module, State}) ->
    case apply_if_exists(Module, terminate_state, [Result, State]) of
        {ok, R} -> R;
        {error, not_exists} -> Result
    end.

apply_if_exists(M, F, A) ->
    case lists:member({F, erlang:length(A)}, M:module_info(exports)) of
        true  -> {ok, erlang:apply(M,F,A)};
        false -> {error, not_exists}
    end.

metrics(Module) -> Module:metrics().

apply(F, Args, Module) ->
    erlang:apply(Module, F, Args).

%% backported from R17

ensure_all_started(Application) ->
    ensure_all_started(Application, temporary).

ensure_all_started(Application, Type) ->
    case ensure_all_started(Application, Type, []) of
        {ok, Started} ->
            {ok, lists:reverse(Started)};
        {error, Reason, Started} ->
            _ = [application:stop(App) || App <- Started],
            {error, Reason}
    end.

ensure_all_started(Application, Type, Started) ->
    case application:start(Application, Type) of
        ok ->
            {ok, [Application | Started]};
        {error, {already_started, Application}} ->
            {ok, Started};
        {error, {not_started, Dependency}} ->
            case ensure_all_started(Dependency, Type, Started) of
                {ok, NewStarted} ->
                    ensure_all_started(Application, Type, NewStarted);
                Error ->
                    Error
            end;
        {error, Reason} ->
            {error, {Application, Reason}, Started}
    end.

load_config([]) ->
    ok;
load_config([File|T]) ->
    case file:consult(mzb_file:expand_filename(File)) of
        {ok, [Config]} ->
            system_log:info("Reading configuration from ~ts", [File]),
            lists:foreach(fun({App, Env}) ->
                                  [application:set_env(App, Key, Val) || {Key, Val} <- Env]
                          end, Config),
            ok;
        {error, enoent} ->
            load_config(T);
        {error, Reason} ->
            system_log:error("Could not open file ~p, reason ~p", [File, Reason])
    end.
