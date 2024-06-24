-module(mzb_lists).

-export(
   [
    choose/1,
    choose/2,
    pmap/2,
    enumerate/1,
    uniq/1
   ]).

choose([]) -> erlang:error(badarg);
choose(List) -> lists:nth(crypto:rand_uniform(1, length(List) + 1), List).

choose(N, List) -> taken(List, N).

taken(L, N) ->
     Len = length(L),
     PropL = lists:zip(lists:seq(1, Len), L),
     taken(PropL, N, Len, []).

taken([], _, _, L) -> L;
taken(_, 0, _, L) -> L;
taken([{_,V}], _, _, L) -> [V | L];
taken(Ps, N, Len, L) ->
    {K1, V1} = lists:nth(crypto:rand_uniform(1, Len), Ps),
    taken(proplists:delete(K1, Ps), N-1, Len-1, [V1 | L]).

pmap(Fun, List) ->
    Self = self(),
    Refs = lists:map(fun (Element) ->
        Ref = erlang:make_ref(),
        _ = mzb_spawn:spawn_link(fun () ->
            Res = try
                {Ref, {ok, Fun(Element)}}
            catch
                C:E:ST -> {Ref, {exception, {C,E,ST}}}
            end,
            Self ! Res
        end),
        Ref
    end, List),
    pmap_results(Refs, []).

pmap_results([], Res) -> lists:reverse(Res);
pmap_results([Ref|T], Res) ->
    receive
        {Ref, {ok, R}} ->
            pmap_results(T, [R|Res]);
        {Ref, {exception, {C,E,ST}}} ->
            erlang:raise(C, E, ST)
    end.

enumerate(List) when is_list(List) ->
    lists:zip(lists:seq(0, length(List) - 1), List).

uniq(List) when is_list(List) ->
    uniq(List, [], sets:new()).

uniq([], Result, _Set) -> lists:reverse(Result);
uniq([X | Rest], Result, Set) ->
    case sets:is_element(X, Set) of
        false -> uniq(Rest, [X | Result], sets:add_element(X, Set));
        true -> uniq(Rest, Result, Set)
    end.
