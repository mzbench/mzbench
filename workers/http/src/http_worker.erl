-module(http_worker).

-export([
    initial_state/0, metrics/0
]).

-export([
    connect/4, disconnect/2,
    set_options/3, set_headers/3,
    get/3, post/4, put/4, set_prefix/3
]).

-type meta() :: [{Key :: atom(), Value :: any()}].
-type http_options() :: list().

-record(state, {
    connection = undefined,
    prefix = "default",
    headers = [] :: [{HeaderName :: binary(), Value :: binary()}],
    options = [] :: http_options()
}).

-type state() :: #state{}.

-define(TIMED(Name, Expr),
    (fun() ->
        StartTime = os:timestamp(),
        Result = Expr,
        Value = timer:now_diff(os:timestamp(), StartTime),
        mzb_metrics:notify({Name, histogram}, Value),
        Result
    end)()).

-spec initial_state() -> state().
initial_state() ->
    application:set_env(hackney, use_default_pool, false),
    #state{}.

-spec metrics() -> list().
metrics() -> metrics("default").

metrics(Prefix) ->
    [
        {group, "HTTP (" ++ Prefix ++ ")", [
            {graph, #{title => "HTTP Response",
                      units => "N",
                      metrics => [{Prefix ++ ".http_ok", counter}, {Prefix ++ ".http_fail", counter}, {Prefix ++ ".other_fail", counter}]}},
            {graph, #{title => "Latency",
                      units => "microseconds",
                      metrics => [{Prefix ++ ".latency", histogram}]}}
        ]}
    ].

-spec set_prefix(state(), meta(), string()) -> {nil, state()}.
set_prefix(State, _Meta, NewPrefix) ->
    mzb_metrics:declare_metrics(metrics(NewPrefix)),
    {nil, State#state{prefix = NewPrefix}}.

-spec disconnect(state(), meta()) -> {nil, state()}.
disconnect(#state{connection = Connection} = State, _Meta) ->
    hackney:close(Connection),
    {nil, State}.

-spec connect(state(), meta(), string() | binary(), integer()) -> {nil, state()}.
connect(State, Meta, Host, Port) when is_list(Host) ->
    connect(State, Meta, list_to_binary(Host), Port);
connect(State, _Meta, Host, Port) ->
    {ok, ConnRef} = hackney:connect(hackney_tcp, Host, Port, []),
    {nil, State#state{connection = ConnRef}}.

-spec set_options(state(), meta(), http_options()) -> {nil, state()}.
set_options(State, _Meta, NewOptions) ->
    ok = hackney:setopts(State#state.connection, NewOptions),
    {nil, State}.

set_headers(State, _Meta, Headers) ->
    BinHeaders = lists:map(fun(Header) ->
        HBin = unicode:characters_to_binary(Header),
        [ Name, Value ] = binary:split(HBin, <<":">>),
        { Name, Value }
    end, Headers),
    { nil, State#state{ headers = BinHeaders }}.

-spec get(state(), meta(), string() | binary()) -> {nil, state()}.
get(State, Meta, Endpoint) when is_list(Endpoint) ->
    get(State, Meta, list_to_binary(Endpoint));
get(State, _Meta, Endpoint) ->
    #state{
        connection = Connection,
        headers = Headers
    } = State,
    logger:info("Request"),
    Response = ?TIMED(prefix(State, ".latency"), hackney:send_request(Connection,
        {get, Endpoint, Headers, <<>>})),
    {nil, record_response(Response, State)}.

-spec post(state(), meta(), string() | binary(), iodata()) -> {nil, state()}.
post(State, Meta, Endpoint, Payload) when is_list(Endpoint) ->
    post(State, Meta, list_to_binary(Endpoint), Payload);
post(State, _Meta, Endpoint, Payload) ->
    #state{
        connection = Connection,
        headers = Headers
    } = State,
    Response = ?TIMED(prefix(State, ".latency"), hackney:send_request(Connection,
        {post, Endpoint, Headers, list_to_binary(Payload)})),
    {nil, record_response(Response, State)}.

-spec put(state(), meta(), string() | binary(), iodata()) -> {nil, state()}.
put(State, Meta, Endpoint, Payload) when is_list(Endpoint) ->
    put(State, Meta, list_to_binary(Endpoint), Payload);
put(State, _Meta, Endpoint, Payload) ->
    #state{
        connection = Connection,
        headers = Headers
    } = State,
    Response = ?TIMED(prefix(State, ".latency"), hackney:send_request(Connection,
        {put, Endpoint, Headers, Payload})),
    {nil, record_response(Response, State)}.

record_response(Response, State) ->
    case Response of
        {ok, 200, _, NewConnection} ->
            hackney:body(NewConnection),
            mzb_metrics:notify({prefix(State, ".http_ok"), counter}, 1),
            State#state{ connection = NewConnection};
        {ok, _, _, NewConnection} ->
            hackney:body(NewConnection),
            mzb_metrics:notify({prefix(State, ".http_fail"), counter}, 1),
            State#state{ connection = NewConnection};
        E ->
            lager:error("hackney:request failed: ~p", [E]),
            mzb_metrics:notify({prefix(State, ".other_fail"), counter}, 1),
            State
    end.

prefix(State, Suffix) ->
    State#state.prefix ++ Suffix.