-module(mzb_script_validator).

-export([validate/1, read_and_validate/2]).

-include_lib("mzbench_language/include/mzbl_types.hrl").

read_and_validate(ScriptFileName, Env) ->
    try
        {ok, WorkerDirs} = application:get_env(mzbench, workers_dirs),
        Nodes = [node()|mzb_interconnect:nodes()],
        AutoEnv = [{"nodes_num", max(length(Nodes) - 1, 1)},
                   {"bench_script_dir", filename:dirname(ScriptFileName)},
                   {"bench_workers_dir", WorkerDirs}],
        Body = mzbl_script:read(ScriptFileName),
        {ok, Warnings} = validate(Body),
        {ok, Warnings, Body, AutoEnv ++ Env}
    catch
        C:{read_file_error, File, E} = Error:ST ->
            Message = mzb_string:format(
                "Failed to read file ~ts: ~ts",
                [File, file:format_error(E)]),
            {error, C, Error, ST, [Message]};
        C:{parse_error, {LineNumber, erl_parse, E}} = Error:ST ->
            Message = mzb_string:format(
                "Failed to parse script ~ts:~nline ~p: ~ts",
                [ScriptFileName, LineNumber, [E]]),
            {error, C, Error, ST, [Message]};
        C:{parse_error,{expected, E, {{line, LineNumber}, {column, ColumnNumber}}}} = Error:ST ->
            Message = mzb_string:format(
                "Failed to parse script ~ts:~nline ~p, column ~p: ~p",
                [ScriptFileName, LineNumber, ColumnNumber, E]),
            {error, C, Error, ST, [Message]};
        C:{invalid_operation_name, Name} = Error:ST ->
            {error, C, Error, ST,
                [mzb_string:format("Script ~s is invalid:~nInvalid operation name ~p~n",
                    [ScriptFileName, Name])]};
        C:{error, {validation, VM}} = Error:ST when is_list(VM) ->
            Messages = [mzb_string:format("Script ~s is invalid:~n", [ScriptFileName]) | VM],
            {error, C, Error, ST, Messages};
        C:Error:ST ->
            Message = mzb_string:format(
                "Script ~ts is invalid:~nError: ~p~n~nStacktrace for the curious: ~p",
                [ScriptFileName, Error, ST]),
            {error, C, Error, ST, [Message]}
    end.

validate(Script) ->
    Script2 = mzbl_script:enumerate_pools(Script),

    case mzbl_typecheck:check(Script2, list) of
        {false, Reason, undefined} ->
            erlang:error({error, {validation, [mzb_string:format("Type error ~p", [Reason])]}});
        {false, Reason, Location} ->
            erlang:error({error, {validation, [mzb_string:format("~tsType error ~p", [Location, Reason])]}});
        _ -> []
    end,

    Errors = lists:foldl(
        fun (#operation{name = include_resource, args = [_Name, Path]}, Acc) ->
                validate_resource_filename(Path) ++ Acc;
            (#operation{name = include_resource, args = [_Name, Path, _Type]}, Acc) ->
                validate_resource_filename(Path) ++ Acc;
            (#operation{name = assert} = Op, Acc) ->
                mzbl_asserts:validate(Op) ++ Acc;
            (#operation{name = make_install}, Acc) -> Acc;
            (#operation{name = pre_hook} = Op, Acc) ->
                mzb_hooks_validator:validate(Op) ++ Acc;
            (#operation{name = post_hook} = Op, Acc) ->
                mzb_hooks_validator:validate(Op) ++ Acc;
            (#operation{name = defaults, args = [DefaultsList]}, Acc) ->
              validate_defaults_list(DefaultsList) ++ Acc;
            (#operation{name = pool} = Pool, Acc) -> validate_pool(Pool) ++ Acc;
            (#operation{name = F, args = A, meta = M}, Acc) ->
                [mzb_string:format("~tsUnknown function: ~p/~p",
                    [mzbl_script:meta_to_location_string(M), F, erlang:length(A)])|Acc];
            (T, Acc) ->
                [mzb_string:format("Unexpected top-level term ~p", [T])|Acc]
        end, [], Script2),

    case Errors of
        [] -> ok;
        _ -> erlang:error({error, {validation, lists:reverse(Errors)}})
    end,

    mzb_signal_validation:validate(Script2),
    Warnings = check_deprecated_defaults(Script2),
    {ok, Warnings}.

validate_resource_filename(#operation{name = 'var'}) -> [];
validate_resource_filename(Filename) ->
    case filename:split(Filename) of
        [_] -> [];
        [".", _] -> [];
        [S | _] ->
            case re:run(S, "^https?:", [{capture, first}, caseless]) of
                nomatch -> [mzb_string:format("Invalid resource filename: ~ts", [Filename])];
                {match, _} -> []
            end
    end.

-spec validate_pool(#operation{}) -> [string()].
validate_pool(#operation{name = pool, args = [Opts, Script], meta = Meta} = Op) ->
    Name = proplists:get_value(pool_name, Op#operation.meta),
    {Provider, Worker} = mzbl_script:extract_worker(Opts),
    Size = case lists:keyfind(size, #operation.name, Opts) of
        #operation{args = [X]} -> X;
        _ -> erlang:error({error, {validation,
            [mzb_string:format("~tspool without size", [mzbl_script:meta_to_location_string(Meta)])]}})
    end,
    WorkerStartType =
        case lists:keyfind(worker_start, #operation.name, Opts) of
            #operation{args = [V]} -> V;
            false -> undefined
        end,
    lists:map(
      fun(Msg) -> Name ++ ": " ++ Msg end,
      case Provider:validate(Worker) of
          [] ->
              SizeErr = case Size of
                #operation{name = Var} when Var == var; Var == numvar -> [];
                #operation{name = N, args = A} ->
                    [mzb_string:format("can't use operation ~p with args ~p as pool size.", [N, A])];
                _ -> [mzb_string:format(
                          "size option: expected something integer-like but got ~p.", [Size])
                          || mzb_utility:to_integer_with_default(Size, fail) == fail] ++
                    ["negative size is not allowed." || mzb_utility:to_integer_with_default(Size, fail) < 0]
              end,
              ScriptErr = case mzb_worker_script_validator:validate_worker_script(Script, {Provider, Worker}) of
                  ok -> [];
                  {invalid_script, Errors} -> Errors
              end,
              StartTypeErr = validate_worker_start_type(WorkerStartType),
              SizeErr ++ ScriptErr ++ StartTypeErr;
          Messages -> Messages
      end).

validate_worker_start_type(undefined) -> [];
validate_worker_start_type(#operation{name = pow, args = [_, _, _]}) -> [];
validate_worker_start_type(#operation{name = pow}) ->
    [mzb_string:format("Invalid pow parameters (should be two positive numbers followed by time constant)", [])];
validate_worker_start_type(#operation{name = exp, args = [_, _]}) -> [];
validate_worker_start_type(#operation{name = exp}) ->
    [mzb_string:format("Invalid exp parameters (should be X > 1 followed by time constant)", [])];
validate_worker_start_type(#operation{name = poisson, args = [_]}) -> [];
validate_worker_start_type(#operation{name = poisson, args = Args}) ->
    [mzb_string:format("Invalid poisson arguments (only one arg is allowed, ~p were given)", [erlang:length(Args)])];
validate_worker_start_type(#operation{name = linear, args = [_]}) -> [];
validate_worker_start_type(#operation{name = linear, args = Args}) ->
    [mzb_string:format("Invalid worker start rate arguments: only one arg is allowed, ~p were given", [erlang:length(Args)])];
validate_worker_start_type(Unknown) ->
    [mzb_string:format("Unknown worker start type: ~p", [Unknown])].

-spec validate_defaults_list(list()) -> [string()].
validate_defaults_list(L) ->
  lists:foldl(
    fun ({Name, _Value}, Acc) ->
          case mzbl_typecheck:check(Name, string) of
            {false, Reason, undefined} ->
                [mzb_string:format("Type error ~p", [Reason])|Acc];
            {false, Reason, Location} ->
                [mzb_string:format("~tsType error ~p", [Location, Reason])|Acc];
            _ -> Acc
          end;
        (Term, Acc) ->
          [mzb_string:format("Invalid term in the list of default values was encountered: ~p", [Term])|Acc]
    end, [], L).

-spec check_deprecated_defaults(script_expr()) -> [string()].
check_deprecated_defaults(Script) ->
    VarsWithDefault = mzbl_ast:fold(
        fun (#operation{name = VarType, args = Args} = Op, Acc) when VarType == var; VarType == numvar ->
                case length(Args) of
                    2 -> [Op | Acc];
                    _ -> Acc
                end;
            (_, Acc) -> Acc
        end, [], Script),
    
    lists:map(
        fun (#operation{args = [Name|_], meta = M}) ->
            mzb_string:format("~tsWarning: ~ts uses a deprecated default value declaration format", [mzbl_script:meta_to_location_string(M), Name])
        end, lists:reverse(VarsWithDefault)).
