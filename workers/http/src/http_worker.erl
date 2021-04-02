-module(http_worker).

-export([
    initial_state/0, metrics/0
]).

-export([
    connect/4, connect_https/4,
    disconnect/2,
    set_options/3, set_headers/3,
    set_timeout/3,
    get/3, post/4, put/4, set_prefix/3
]).

-type meta() :: [{Key :: atom(), Value :: any()}].
-type http_options() :: list().

-record(state, {
    connection = undefined,
    prefix = "default",
    headers = [] :: [{HeaderName :: binary(), Value :: binary()}],
    options = [] :: http_options(),
    def_options = #{} :: maps()
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

-on_load(init/0).

init() ->
    application:set_env(hackney, use_default_pool, false).

-spec initial_state() -> state().
initial_state() ->
    #state{}.

-spec metrics() -> list().
metrics() -> metrics("default").

metrics(Prefix) ->
    [
        {group, "HTTP (" ++ Prefix ++ ")", [
            {graph, #{
                title => "HTTP Response",
                units => "N",
                metrics => [{Prefix ++ ".http_ok", counter}, {Prefix ++ ".http_fail", counter}, {Prefix ++ ".other_fail", counter}]}},
            {graph, #{
                title => "Latency",
                units => "microseconds",
                metrics => [{Prefix ++ ".latency", histogram}]}}
        ]}
    ].

-spec set_prefix(state(), meta(), string()) -> {nil, state()}.
set_prefix(State, _Meta, NewPrefix) ->
    mzb_metrics:declare_metrics(metrics(NewPrefix)),
    {nil, State#state{prefix = NewPrefix}}.

-spec disconnect(state(), meta()) -> {nil, state()}.
disconnect(State = #state{ connection = undefined }, _) -> { nil, State };
disconnect(#state{connection = Connection} = State, _Meta) ->
    hackney:close(Connection),
    {nil, State}.

-spec connect(state(), meta(), string() | binary(), integer()) -> {nil, state()}.
connect(State, Meta, Host, Port) when is_list(Host) ->
    connect(State, Meta, list_to_binary(Host), Port);
connect(State, _Meta, Host, Port) ->
    Connection = case hackney:connect(hackney_ssl, Host, Port, []) of
        {ok, ConnRef} -> ConnRef;
        { error, _ } ->
            mzb_metrics:notify({prefix(State, ".other_fail"), counter}, 1),
            undefined
    end,
    {nil, State#state{connection = Connection}}.

-spec connect(state(), meta(), string() | binary(), integer()) -> {nil, state()}.
connect_https(State, Meta, Host, Port) when is_list(Host) ->
    connect_https(State, Meta, list_to_binary(Host), Port);
connect_https(State, _Meta, Host, Port) ->
    Connection = case hackney:connect(hackney_tcp, Host, Port, []) of
        {ok, ConnRef} -> ConnRef;
        { error, _ } ->
            mzb_metrics:notify({prefix(State, ".other_fail"), counter}, 1),
            undefined
    end,
    {nil, State#state{connection = Connection}}.

set_timeout(State, _Meta, Timeout) ->
    NewState = set_def_option(State, recv_timeout, Timeout),
    { nil, NewState }.

-spec set_options(state(), meta(), http_options()) -> {nil, state()}.
set_options(State, _Meta, NewOptions) ->
    Opts = merge_options(State, NewOptions),
    ok = hackney:setopts(State#state.connection, Opts),
    {nil, State#state{ options = Opts }}.

set_headers(State, _Meta, Headers) ->
    BinHeaders = lists:map(fun(Header) ->
        HBin = unicode:characters_to_binary(Header),
        [ Name, Value ] = binary:split(HBin, <<":">>),
        { Name, Value }
    end, Headers),
    { nil, State#state{ headers = BinHeaders }}.

-spec get(state(), meta(), string() | binary()) -> {nil, state()}.
get(State = #state{ connection = undefined }, _, _) -> { nil, State };
get(State, Meta, Endpoint) when is_list(Endpoint) ->
    get(State, Meta, list_to_binary(Endpoint));
get(State, _Meta, Endpoint) ->
    #state{
        connection = Connection,
        headers = Headers
    } = State,
    % logger:info("Request"),
    Response = ?TIMED(prefix(State, ".latency"), hackney:send_request(Connection,
        {get, Endpoint, Headers, <<>>})),
    {nil, record_response(Response, State)}.

-spec post(state(), meta(), string() | binary(), iodata()) -> {nil, state()}.
post(State = #state{ connection = undefined }, _, _, _) -> { nil, State };
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
put(State = #state{ connection = undefined }, _, _, _) -> { nil, State };
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

% Internal

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

set_def_option(State, Name, Value) ->
    DefOpt = State#state.def_options,
    Opts = merge_options(State, [{Name, Value}]),
    case State#state.connection of
        undefined -> ok;
        Conn ->
            ok = hackney:setopts(Conn, Opts)
    end,
    State#state{
        def_options = maps:put(Name, Value, DefOpt),
        options = Opts
    }.

merge_options(State, Options) ->
    maps:to_list(maps:merge(State#state.def_options, maps:from_list(Options))).
