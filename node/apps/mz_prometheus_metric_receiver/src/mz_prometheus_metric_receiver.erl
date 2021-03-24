-module(mz_prometheus_metric_receiver).

%% Worker API
-export([
  initial_state/0, metrics/0, terminate_state/2
]).

%% Worker commands
-export([
  enable_metrics/3,
  load/3
]).

-define(REQUEST_INTERVAL, 1000).

-record(state, {
  url :: string(),
  host :: string(),
  metrics_enable = [] :: [fun((Host :: string()) -> [term()])]
}).

%% MZBench worker behaviour

initial_state() ->
  #state{}.

metrics() -> [].

terminate_state(_Reason, _State) -> ok.

enable_metrics(State, _Meta, MetricsList) ->
  Metrics = lists:foldl(fun(Name, Acc) ->
    [ metric_to_func(Name) | Acc ]
  end, State#state.metrics_enable, MetricsList),
  NewState = State#state{ metrics_enable = Metrics },
  { nil, NewState }.

load(State, _Meta, URL) ->
  #{ host := Host } = uri_string:parse(URL),
  MetricFuncs = State#state.metrics_enable,
  Groups = lists:foldl(fun(Func, Acc) ->
    Acc ++ Func(Host)
  end, [], MetricFuncs),
  mzb_metrics:declare_metrics(Groups),
  mz_prometheus_server:enter_loop(?REQUEST_INTERVAL, URL),
  { nil, State }.

metric_to_func("erlang") -> fun mz_prometheus_metrics:erlang_metrics/1;
metric_to_func("mnesia") -> fun mz_prometheus_metrics:mnesia_metrics/1;
metric_to_func("cowboy") -> fun mz_prometheus_metrics:cowboy_metrics/1;
metric_to_func("linux_process") -> fun mz_prometheus_metrics:linux_process_metrics/1;
metric_to_func(Any) -> error({ bad_metric_name, Any }).