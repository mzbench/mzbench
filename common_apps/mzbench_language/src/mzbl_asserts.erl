-module(mzbl_asserts).

-export([
    validate/1,
    init/1,
    update_state/3,
    get_failed/3,
    check_loop_expr/2,
    check_expr/2,
    format_state/1
]).

-include_lib("mzbl_types.hrl").

-spec validate(script_expr()) -> list(string()).
validate(#operation{name = assert, args = [always, Expression], meta = M}) ->
    validate_assert_expr(Expression, M);
validate(#operation{name = assert, args = [Time, Expression], meta = M}) ->
    case mzbl_typecheck:check(Time, time) of
        {false, Reason, _Location} -> [Reason];
        _ -> []
    end
    ++
    validate_assert_expr(Expression, M).

validate_assert_expr(Op, _M) when is_number(Op) or is_list(Op) -> [];
validate_assert_expr(#operation{name = var, args = [_, Default]}, M) ->
    if is_number(Default) -> [];
        true -> invalid_assert(M) end;
validate_assert_expr(#operation{name = numvar}, _M) -> [];
validate_assert_expr(#operation{name = Name, args = [Op1, Op2], meta = M}, _) ->
    validate_assert_op(Name, M) ++ validate_assert_expr(Op1, M) ++ validate_assert_expr(Op2, M);
validate_assert_expr(#operation{name = 'not', args = [Op], meta = M}, _) ->
    validate_assert_expr(Op, M);
validate_assert_expr(#operation{meta = M}, _) ->
    invalid_assert(M);
validate_assert_expr(_Invalid, M) ->
    invalid_assert(M).

invalid_assert(M) -> [mzb_string:format("~tsInvalid assert expression", [mzbl_script:meta_to_location_string(M)])].

validate_assert_op('and', _) -> [];
validate_assert_op('or', _) -> [];
validate_assert_op(gt, _) -> [];
validate_assert_op(lt, _) -> [];
validate_assert_op(gte, _) -> [];
validate_assert_op(lte, _) -> [];
validate_assert_op(eq, _) -> [];
validate_assert_op(ne, _) -> [];
validate_assert_op(Name, M) ->
    [mzb_string:format("~tsInvalid assert operation: ~p",
        [mzbl_script:meta_to_location_string(M), Name])].

-spec get_failed(boolean(), integer(), [map()]) -> [tuple()].
get_failed(IsFinished, Accuracy, State) ->
    Failed = lists:filter(fun (A) -> not check_assert(IsFinished, Accuracy * 1000, A) end, State),
    [{Expr, format_error(A)} || #{assert_expr:= Expr} = A <- Failed].

check_assert(_, Accuracy, #{assert_time:= always, unsatisfy_time:= UTime}) when UTime > Accuracy -> false;
check_assert(_, Accuracy, #{assert_time:= always, satisfy_time:= STime}) when STime > Accuracy -> true;
check_assert(true, _Accuracy, #{assert_time:= always, unsatisfy_time:= UTime}) when UTime > 0 -> false;
check_assert(_, _Accuracy, #{assert_time:= always}) -> true;
check_assert(true, _, #{assert_time:= ExpectedTime, satisfy_time:= STime}) when ExpectedTime > STime -> false;
check_assert(_, _, #{assert_time:= _, satisfy_time:= _}) -> true.

-spec init(list(tuple())) -> [map()].
init(Asserts) ->
    [#{assert_time => always,
       assert_expr => Expr,
       satisfy_time => 0,
       unsatisfy_time => 0} || {always, Expr} <- Asserts] ++
    [#{assert_time => Time * 1000,
       assert_expr => Expr,
       satisfy_time => 0,
       unsatisfy_time => 0} || {#constant{value = Time, units = ms}, Expr} <- Asserts].

-spec update_state(integer(), [map()], [tuple()]) -> [map()].
update_state(TimeSinceCheck, State, Env) ->
    lists:map(
        fun (#{assert_expr:= Expr, satisfy_time:= STime, unsatisfy_time:= UTime} = A) ->
            case check_expr(Expr, Env) of
                true  -> A#{satisfy_time:= STime + TimeSinceCheck};
                false -> A#{unsatisfy_time:= UTime + TimeSinceCheck}
            end
        end, State).

-spec check_loop_expr(list(), [tuple()]) -> boolean().
check_loop_expr(List, Env) ->
    lists:all(fun(Expr) -> check_expr(Expr, Env) end, List).

get_value(V, _) when is_number(V) -> [V];
get_value(Metric, _) when is_list(Metric) -> [V || {_, V} <- mzb_metrics:get_by_wildcard(Metric)];
get_value(Expr, Env) -> [mzbl_interpreter:eval_std(Expr, Env)].

-spec check_expr(any(), [tuple()]) -> boolean().
check_expr(#operation{name = 'not', args = [Exp1]}, Env) -> not check_expr(Exp1, Env);
check_expr(#operation{name = 'and', args = [Exp1, Exp2]}, Env) ->
    case check_expr(Exp1, Env) of
        false -> false;
        true -> check_expr(Exp2, Env)
    end;
check_expr(#operation{name = 'or', args = [Exp1, Exp2]}, Env) ->
    case check_expr(Exp1, Env) of
        true -> true;
        false -> check_expr(Exp2, Env)
    end;
check_expr(#operation{name = Op, args = [Value1, Value2]}, Env) ->
    try {get_value(Value1, Env), get_value(Value2, Env)} of
        {VL1, VL2} -> lists:all(fun(X) -> X end,
                [check_value(Op, V1, V2) || V1 <- VL1, V2 <- VL2])
    catch
        error:{badarg, _Metric, _} -> false
    end.

check_value(gt, V1, V2) -> V1 > V2;
check_value(gte, V1, V2) -> V1 >= V2;
check_value(lt, V1, V2) -> V1 < V2;
check_value(lte, V1, V2) -> V1 =< V2;
check_value(ne, V1, V2) -> V1 /= V2;
check_value(eq, V1, V2) -> V1 == V2.

format_error(#{assert_time:= always, assert_expr:= Expr, unsatisfy_time:= UTime}) ->
    io_lib:format("Assertion: ~ts~nwas expected to hold for whole bench time~n(unsatisfied for ~ts)",
                  [format_expr(Expr), format_time(UTime)]);
format_error(#{assert_time:= ExpectedTime, assert_expr:= Expr, satisfy_time:= STime}) ->
    io_lib:format("Assertion: ~ts~nwas expected to hold for ~ts~nbut held for just ~ts",
                  [format_expr(Expr), format_time(ExpectedTime), format_time(STime)]).

format_expr(Op) when is_list(Op) -> Op;
format_expr(Op) when is_integer(Op) -> integer_to_list(Op);
format_expr(Op) when is_float(Op) -> float_to_list(Op);
format_expr(#operation{name = VarKind, args = Args}) when (VarKind == 'var') or (VarKind == 'numvar')->
    io_lib:format("~p~p", [VarKind, Args]);
format_expr(#operation{name = 'not', args = [Op1]}) ->
    io_lib:format("(not ~ts)", [format_expr(Op1)]);
format_expr(#operation{name = Operation, args = [Op1, Op2]}) ->
    io_lib:format("(~ts ~ts ~ts)", [format_expr(Op1), format_op(Operation), format_expr(Op2)]).

format_op('or') -> "or";
format_op('and') -> "and";
format_op(gt) -> ">";
format_op(lt) -> "<";
format_op(gte) -> ">=";
format_op(lte) -> "<=";
format_op(eq) -> "==";
format_op(ne) -> "!=".

format_time(always) -> "always";
format_time(0) -> "0";
format_time(Time) when Time < 1000000 -> io_lib:format("~bus", [Time]);
format_time(Time) ->
    case {calendar:seconds_to_time(Time div 1000000), Time rem 1000000} of
        {{0, 0, S}, 0} -> io_lib:format("~bs", [S]);
        {{0, 0, S}, Mk} -> io_lib:format("~bs ~ts", [S, format_time(Mk)]);
        {{0, M, S}, 0} -> io_lib:format("~bm ~bs", [M, S]);
        {{0, M, S}, Mk} -> io_lib:format("~bm ~bs ~ts", [M, S, format_time(Mk)]);
        {{H, M, S}, _} -> io_lib:format("~bh ~bm ~bs", [H, M, S])
    end.

-spec format_state([map()]) -> string().
format_state([]) -> "(empty)";
format_state(State) ->
    Lines = lists:map(
        fun (#{assert_time:= ExpectedTime, assert_expr:= Expr,
               satisfy_time:= STime, unsatisfy_time:= UTime}) ->
                io_lib:format("~ts: ~ts / ~ts / ~ts",
                              [format_expr(Expr), format_time(ExpectedTime), format_time(STime), format_time(UTime)])
        end, State),
    string:join(Lines, "\n").
