-module(mz_prometheus_server).

-export([
  start_link/2,
  enter_loop/2
]).

-behaviour(gen_server).
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {
  interval :: pos_integer(),
  host :: string(),
  url :: string()
}).

start_link(Interval, Url) ->
  gen_server:start_link(?MODULE, [ Interval, Url ], []).

enter_loop(Interval, URL) ->
  { ok, State } = init([ Interval, URL ]),
  gen_server:enter_loop(?MODULE, [], State).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([ Interval, URL ]) ->
  #{ host := Host } = uri_string:parse(URL),
  State = #state{
    interval = Interval,
    host = Host,
    url = URL
  },
  {ok, tick(State)}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(tick, State) ->
  NewState = request(State),
  { noreply, tick(NewState)};

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

tick(State) ->
  erlang:send_after(State#state.interval, self(),  tick),
  State.

request(State) ->
  case httpc:request(get, { State#state.url, []}, [], [{ body_format, binary }]) of
    {ok, {{ _, 200, _}, _, Body}} ->
      parse(Body, State);
    { ok, {{ _, Error, _ }, _, Body}} ->
      lager:error("Metrics server returned ~p:~s", [ Error, Body ]);
    { error, Reason} ->
      lager:error("Can't connect to metrics server: ~p", [ Reason ])
  end,
  State.

parse(Body, State) ->
  Lines = binary:split(Body, [<<"\n">>], [ global ]),
  Host = State#state.host ++ ".",
  { ok, Re } = re:compile("^(.+(\{.*\})?) (.+)"),
  Metrics = lists:foldl(fun(Line, Acc) ->
    case Line of
      <<>> -> Acc;
      <<"# ", _/binary>> -> Acc;
      _ ->
        [_,NamePart,_,ValuePart] = case re:run(Line, Re) of
          { match, Any } -> Any;
          nomatch ->
            error({cant_process, Line })
        end,
        Name = binary:part(Line, NamePart),
        Value = binary_to_number(binary:part(Line, ValuePart)),
        [{ Host ++ binary_to_list(Name),Value } | Acc ]
    end
  end, [], Lines),
  lager:info("Metrics values ~p", [ Metrics ]),
  lists:foreach(fun({Metric, Value}) ->
    mzb_metrics:notify({ Metric, gauge }, Value)
  end, Metrics),
  ok.

binary_to_number(Bin) ->
  try
    binary_to_integer(Bin)
  catch
    error:badarg ->
      binary_to_float(Bin)
  end.

