-module(mz_prometheus_server).

-export([
  start_link/3,
  enter_loop/3
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
  name :: string(),
  url :: string()
}).

start_link(Name, URL, Interval) ->
  gen_server:start_link(?MODULE, [ Name, URL, Interval ], []).

enter_loop(Name, URL, Interval) ->
  { ok, State } = init([ Name, URL, Interval]),
  gen_server:enter_loop(?MODULE, [], State).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([ Name, URL, Interval ]) ->
  State = #state{
    interval = Interval,
    name = Name ++ ".",
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
  RecName = State#state.name,
  { ok, Re } = re:compile("^(.+(\{.*\})?) (.+)"),
  Metrics = lists:foldl(fun(Line, Acc) ->
    case Line of
      <<>> -> Acc;
      <<"# ", _/binary>> -> Acc;
      _ ->
        case re:run(Line, Re) of
          { match, [_,NamePart,_,ValuePart] } ->
            Name = binary:part(Line, NamePart),
            Value = binary_to_number(binary:part(Line, ValuePart)),
            [ {RecName ++ binary_to_list(Name),Value } | Acc ];
          nomatch ->
            logger:error("Prometheus metric receiver ~s: Can't process metric line: ~s .It was ignored", [ RecName, Line ]),
            Acc
        end
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

