-module (mondemand_backend_stats_rrd_recent).

-behaviour (gen_server).

%% API
-export ([ start_link/0,
           add/8,
           check/2,
           cleanup/0
         ]).

%% gen_server callbacks
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3
         ]).

-record (state, { timer }).
-define (TABLE, md_be_stats_rrd_recent).

start_link () ->
  gen_server:start_link ({local, ?MODULE},?MODULE, [], []).


add (Index, Timestamp, ProgId, MetricType,
     MetricName, MetricValue, Host, Context) ->
  ets:insert (?TABLE, {{Timestamp, Index}, ProgId,
                       MetricType, MetricName, MetricValue, Host, Context}).

check (Index, Timestamp) ->
  case ets:lookup (?TABLE, {Timestamp, Index}) of
    [] ->
      error_logger:error_msg (
        "Couldn't find recent request for index ~p and timestamp ~p",
        [Index, Timestamp]),
      error;
    L when is_list (L) ->
      [
        begin
          ContextStr =
            list_to_binary (
              mondemand_server_util:join (
                [ mondemand_server_util:join ([K,V],"=") || {K, V} <- Context ],
                ",")
            ),
          error_logger:error_msg (
            "Program ~s on host ~s with context [~s] may be "
            "writing ~s:~s:~p too fast",
            [ProgId,Host,ContextStr,MetricType,MetricName,MetricValue])
        end
        || {_,ProgId,MetricType,MetricName,MetricValue,Host,Context} <- L
      ],
      ok
  end.

cleanup () ->
  ets:select_delete (?TABLE, ms()).

ms () ->
  % Table stores
  %
  % {{Timestamp, Index}, ProgId, MetricType, MetricName, MetricValue,
  %  Host, Context}
  %
  % I want to match anything which is older than 5 seconds ago, so get
  % seconds since epoch and remove anything older
  %
  % I created this with
  %
  % ets:fun2ms (fun ({{TS,_},_,_,_,_,_,_}) when TS < 5 -> true;
  %                 (_) -> false
  %             end).
  %
  % then added the seconds since epoch part
  %
  [ { {{'$1','_'},'_','_','_','_','_','_'},
      [{'<','$1', mondemand_server_util:seconds_since_epoch() - 5 }],
      [true] },
    { '_',
      [],
      [false] }
  ].

%%====================================================================
%% gen_server callbacks
%%====================================================================
init ([]) ->
  % so our terminate/2 always gets called
  process_flag( trap_exit, true ),
  {ok, TRef} = timer:apply_interval (1000, ?MODULE, cleanup, []),

  ets:new (?TABLE, [set, public, named_table, {keypos, 1}]),

  { ok, #state { timer = TRef } }.

handle_call (Request, From, State = #state {}) ->
  error_logger:warning_msg ("~p : Unrecognized call ~p from ~p~n",
                            [?MODULE, Request, From]),
  { reply, ok, State }.

handle_cast (Request, State = #state {}) ->
  error_logger:warning_msg ("~p : Unrecognized cast ~p~n",[?MODULE, Request]),
  { noreply, State }.

handle_info (Request, State = #state { }) ->
  error_logger:warning_msg ("~p : Unrecognized info ~p~n",[?MODULE, Request]),
  { noreply, State }.

terminate (_Reason, #state {}) ->
  ok.

code_change (_OldVsn, State, _Extra) ->
  { ok, State }.
