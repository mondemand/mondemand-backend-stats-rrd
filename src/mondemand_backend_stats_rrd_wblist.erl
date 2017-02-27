-module (mondemand_backend_stats_rrd_wblist).

-behaviour (gen_server).

%% API
-export ([ start_link/2,
           is_prog_id_allowed/1
         ]).

%% gen_server callbacks
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3
         ]).

-record (state, {}).
-define (NAME, mdbes_rrd_wblist).
-define (TABLE, mdbes_rrd_wblist).
-record (entry, { prog_id,
                  is_blacklisted = false,
                  is_whitelisted = false
                }).

%%====================================================================
%% API
%%====================================================================
start_link (WhitelistedProgIds, BlacklistedProgIds) ->
  gen_server:start_link ({local, ?NAME}, ?MODULE,
                         [WhitelistedProgIds, BlacklistedProgIds],
                         []).

% given a program id, check to see if it exists in the whitelist/blacklist
% table.  The rules are
%
% 1. if there are whitelists, then it must be in the whitelist in order
%    to be allowed
% 2. if there is no whitelist, it must not be in the blacklist in order
%    to be allowed
%
is_prog_id_allowed (ProgId) ->
  Entry = ets:lookup (?TABLE, mondemand_util:binaryify(ProgId)),

  % if we have any whitelists, we only check for whitelisted entries
  % and any others are disallowed
  case mondemand_global:get (mdbes_rrd_has_whitelist) of
    true ->
      % we have whitelists, see if this entry is one of them, otherwise
      % it's not allowed
      case Entry of
        [#entry { is_whitelisted = true }] -> true;
        _ -> false
      end;
    false ->
      % we don't have any whitelists, so check if this entry is blacklisted,
      % if it is not we allow it
      case Entry of
        [#entry { is_blacklisted = true }] -> false;
        _ -> true
      end
  end.

%%====================================================================
%% gen_server callbacks
%%====================================================================
init ([WhitelistedProgIds, BlacklistedProgIds]) ->

  process_flag(trap_exit, true),

  % keep track of a global boolean to know if there is a whitelist at
  % all
  mondemand_global:put (mdbes_rrd_has_whitelist, WhitelistedProgIds =/= []),

  ets:new(?TABLE, [set, protected, named_table, {keypos, 2}]),
  case WhitelistedProgIds of
    [] ->
      [
        ets:insert (?TABLE,
                    #entry { prog_id = mondemand_util:binaryify (B),
                             is_blacklisted = true }
                   )
        || B
        <- BlacklistedProgIds
      ];
    WL ->
      [
        ets:insert (?TABLE,
                    #entry { prog_id = mondemand_util:binaryify (W),
                             is_whitelisted = true }
                   )
        || W
        <- WL
      ]
  end,
  {ok, #state {} }.

handle_call (_Request, _From, State) ->
  { reply, ok, State }.

handle_cast (_Request, State) ->
  { noreply, State }.

handle_info (_Request, State) ->
  { noreply, State }.

terminate (_Reason, #state { }) ->
  ets:delete (?TABLE),
  ok.

code_change (_OldVsn, State, _Extra) ->
  { ok, State }.

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

setup (WL, BL) ->
  {ok, Pid} = start_link (WL, BL),
  Pid.

cleanup (Pid) ->
  exit (Pid, normal),
  timer:sleep(100), % need a slight pause otherwise below might fail
  ?assertEqual(false, is_process_alive(Pid)).

wblist_test_ () ->
  { inorder,
    [
      { "whitelist",
        { setup,
          spawn,
          fun () -> setup ([foo,bar],[]) end,
          fun cleanup/1,
          [
            ?_assertEqual (false, is_prog_id_allowed(baz)),
            ?_assertEqual (false, is_prog_id_allowed("baz")),
            ?_assertEqual (false, is_prog_id_allowed(<<"baz">>)),
            ?_assertEqual (true, is_prog_id_allowed(bar)),
            ?_assertEqual (true, is_prog_id_allowed("bar")),
            ?_assertEqual (true, is_prog_id_allowed(<<"bar">>)),
            ?_assertEqual (true, is_prog_id_allowed(foo)),
            ?_assertEqual (true, is_prog_id_allowed("foo")),
            ?_assertEqual (true, is_prog_id_allowed(<<"foo">>))
          ]
        }
      },
      { "blacklist",
        { setup,
          spawn,
          fun () -> setup ([], [foo,bar]) end,
          fun cleanup/1,
          [
            ?_assertEqual (true, is_prog_id_allowed(baz)),
            ?_assertEqual (true, is_prog_id_allowed("baz")),
            ?_assertEqual (true, is_prog_id_allowed(<<"baz">>)),
            ?_assertEqual (false, is_prog_id_allowed(bar)),
            ?_assertEqual (false, is_prog_id_allowed("bar")),
            ?_assertEqual (false, is_prog_id_allowed(<<"bar">>)),
            ?_assertEqual (false, is_prog_id_allowed(foo)),
            ?_assertEqual (false, is_prog_id_allowed("foo")),
            ?_assertEqual (false, is_prog_id_allowed(<<"foo">>))
          ]
        }
      },
      % should be the same as whitelist
      { "both",
        { setup,
          spawn,
          fun () -> setup ([foo,bar],[baz]) end,
          fun cleanup/1,
          [
            ?_assertEqual (false, is_prog_id_allowed(baz)),
            ?_assertEqual (false, is_prog_id_allowed("baz")),
            ?_assertEqual (false, is_prog_id_allowed(<<"baz">>)),
            ?_assertEqual (true, is_prog_id_allowed(bar)),
            ?_assertEqual (true, is_prog_id_allowed("bar")),
            ?_assertEqual (true, is_prog_id_allowed(<<"bar">>)),
            ?_assertEqual (true, is_prog_id_allowed(foo)),
            ?_assertEqual (true, is_prog_id_allowed("foo")),
            ?_assertEqual (true, is_prog_id_allowed(<<"foo">>))
          ]
        }
      },
      { "neither",
        { setup,
          spawn,
          fun () -> setup ([],[]) end,
          fun cleanup/1,
          [
            ?_assertEqual (true, is_prog_id_allowed(baz)),
            ?_assertEqual (true, is_prog_id_allowed("baz")),
            ?_assertEqual (true, is_prog_id_allowed(<<"baz">>)),
            ?_assertEqual (true, is_prog_id_allowed(bar)),
            ?_assertEqual (true, is_prog_id_allowed("bar")),
            ?_assertEqual (true, is_prog_id_allowed(<<"bar">>)),
            ?_assertEqual (true, is_prog_id_allowed(foo)),
            ?_assertEqual (true, is_prog_id_allowed("foo")),
            ?_assertEqual (true, is_prog_id_allowed(<<"foo">>))
          ]
        }
      }
    ]
  }.

gen_server_coverage_test_ () ->
  [
    ?_assertEqual ({reply, ok, #state{}}, handle_call(ok, ok, #state{})),
    ?_assertEqual ({noreply, #state{}}, handle_cast(ok, #state{})),
    ?_assertEqual ({noreply, #state{}}, handle_info(ok, #state{})),
    ?_assertEqual ({ok, #state{}}, code_change(ok, #state{}, ok))
  ].

-endif.
