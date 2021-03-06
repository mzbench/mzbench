-module(mzb_gc).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(s, {
    sleep = undefined :: undefined | integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Sleep) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Sleep], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


init([undefined]) -> ignore;
init([Sleep]) ->
    self() ! trigger,
    {ok, #s{sleep = Sleep}}.

handle_call(_Request, _From, State) ->
   {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(trigger, State = #s{sleep = Sleep}) ->
    lists:foreach(
        fun (P) ->
            _ = (catch erlang:garbage_collect(P)),
            timer:sleep(Sleep)
        end, erlang:processes()),
    self() ! trigger,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

