-module (mondemand_backend_stats_rrd_filecache).

-behaviour (gen_server).

-include ("mondemand_backend_stats_rrd_internal.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export ([ start_link/4,
           check_cache/6,
           check_cache/1,
           save_cache/0,
           show_errors/0,
           clear_errors/0,
           clear_entry/1,
           mark_error/2,
           mark_created/1,
           print_stats/0,
           state_stats/0,
           blacklist_all/0,
           blacklist_prog_id/1,
           whitelist_all/0,
           whitelist_prog_id/1
         ]).

%% gen_server callbacks
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3
         ]).

-record (state, { delay, interval, timer, file, host_dir, aggregate_dir }).
-define (NAME, mdbes_rrd_filecache).
-define (TABLE, mdbes_rrd_filecache).
% FIXME: temporary for upgrade from pre-1.0.0
-define (OLD_TABLE, md_be_stats_rrd_filecache).

%%====================================================================
%% API
%%====================================================================
start_link (FileNameCacheFile, HostDir, AggregateDir, ErrorTimeout) ->
  mondemand_global:put (md_be_rrd_dirs, {HostDir, AggregateDir}),
  mondemand_global:put (error_timeout, ErrorTimeout),
  gen_server:start_link ({local, ?NAME},?MODULE,
                         [FileNameCacheFile, HostDir, AggregateDir],[]).

%% return {ok, FilePath, FileKey} or {error, Reason, FileKey}
check_cache (Prefix, ProgIdIn, MetricType,
             MetricNameIn, HostIn, ContextIn) ->
  FileKey =
    mondemand_backend_stats_rrd_key:new (ProgIdIn, MetricType, MetricNameIn,
                                         HostIn, ContextIn),
  CurrentTimestamp = os:timestamp(),

  case ets:lookup (?TABLE, FileKey) of
    [{_, FP, State, Timestamp}] ->
      case State of
        created -> {ok, FP, FileKey};
        creating -> {error, creating, FileKey};
        clearing -> {error, clearing, FileKey};
        blacklisted -> {error, blacklisted, FileKey};
        {error, E} ->
          check_error_duration (FileKey, E, Timestamp, CurrentTimestamp)
      end;
    [] ->
      % calculate the context for building, it includes the directory
      % and filename for the legacy rrd files (which are still the primary
      % files)
      BuilderContext =
        #mdbes_rrd_builder_context { legacy_rrd_file_dir = LegacyFileDir,
                                     legacy_rrd_file_name= LegacyFileName
                                   } =
          mondemand_backend_stats_rrd_builder:calculate_context (
            Prefix, FileKey, get_dirs()
          ),

      % get a complete path for the cache as that's what rrdcached will expect
      RRDFile = filename:join ([LegacyFileDir, LegacyFileName]),

      case mondemand_backend_stats_rrd_wblist:is_prog_id_allowed (ProgIdIn) of
        false ->
          % store in the cache in the creating state
          ets:insert (?TABLE, {FileKey, RRDFile, blacklisted, CurrentTimestamp}),
          {error, blacklisted, FileKey};
        true ->
          % store in the cache in the creating state
          ets:insert (?TABLE, {FileKey, RRDFile, creating, CurrentTimestamp}),

          % then ask the builder to build it asyncronously
          mondemand_backend_stats_rrd_builder:build (BuilderContext),

          % and return to caller that we are creating so no writes are attempted
          % until it's created
          {error, creating, FileKey}
      end
  end.

check_cache (FileNameOrKey) ->
  Key = filename_to_key (FileNameOrKey),
  ets:lookup (?TABLE, Key).

save_cache () ->
  File = gen_server:call (?NAME, {cache_file}),
  save_cache (File).

show_errors () ->
  ets:select (?TABLE,
              ets:fun2ms(fun({K,_,{error,E},_}) -> {K,E} end)).

clear_errors () ->
  ets:select_delete (?TABLE,
                     ets:fun2ms (fun({_,_,{error,_},_}) -> true end)).

clear_entry (FileNameOrKey) ->
  Key = filename_to_key (FileNameOrKey),
  ets:delete(?TABLE, Key).

mark_error (FilenameOrKey, Error)  ->
  update_state_and_timestamp (FilenameOrKey, {error, Error}).

mark_created (FilenameOrKey) ->
  update_state (FilenameOrKey, created).

print_stats() ->
  [ io:format ("~-15s : ~b~n",[atom_to_list(K), V]) || {K,V} <- state_stats() ],
  ok.

state_stats() ->
  [ { created,
      ets:select_count (?TABLE, ets:fun2ms(fun({_,_,created,_}) -> true end))
    },
    { error,
      ets:select_count (?TABLE, ets:fun2ms(fun({_,_,{error,_},_}) -> true end))
    },
    { creating,
      ets:select_count (?TABLE, ets:fun2ms(fun({_,_,creating,_}) -> true end))
    },
    { clearing,
      ets:select_count (?TABLE, ets:fun2ms(fun({_,_,clearing,_}) -> true end))
    },
    { blacklisted,
      ets:select_count (?TABLE, ets:fun2ms(fun({_,_,blacklisted,_}) -> true end))
    }
  ].

blacklist_all () ->
  ets:foldl(fun ({K,_,_,_},A) ->
              update_state_and_timestamp (K, blacklisted),
              A + 1
            end,
            0,
            ?TABLE).

blacklist_prog_id (ProgId) ->
  ProgIdBin = mondemand_util:binaryify(ProgId),
  ets:foldl(fun ({K = {P,_,_,_,_},_,_,_},A) ->
              case P =:= ProgIdBin of
                true ->
                  update_state_and_timestamp (K, blacklisted),
                  A + 1;
                false ->
                  A
              end
            end,
            0,
            ?TABLE).

whitelist_all () ->
  ets:foldl(fun ({K,_,_,_},A) ->
              update_state_and_timestamp (K, created),
              A + 1
            end,
            0,
            ?TABLE).

whitelist_prog_id (ProgId) ->
  ProgIdBin = mondemand_util:binaryify(ProgId),
  ets:foldl(fun ({K = {P,_,_,_,_},_,_,_},A) ->
              case P =:= ProgIdBin of
                true ->
                  update_state_and_timestamp (K, created),
                  A + 1;
                false ->
                  A
              end
            end,
            0,
            ?TABLE).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init ([FileNameCacheFile, HostDir, AggregateDir]) ->
  Interval = 3600,         % flush each hour
  Delay = Interval * 1000, % delay is in milliseconds

  % so our terminate/2 always gets called
  process_flag( trap_exit, true ),

  % setup checking for journals
  { ok, TRef } = timer:apply_interval (Delay, ?MODULE, save_cache, []),

  % attempt to load the file cache from a file at startup
  case load_cache (FileNameCacheFile) of
    true -> ok;
    false ->
      % it it failed we'll recreate it
      ets:new (?TABLE, [set, public, named_table, {keypos, 1}])
  end,

  { ok,
    #state { delay = Delay,
             interval = Interval,
             timer = TRef,
             file = FileNameCacheFile,
             host_dir = HostDir,
             aggregate_dir = AggregateDir
    }
  }.

handle_call ({cache_file}, _, State = #state {file = File}) ->
  {reply, File, State};
handle_call (Request, From, State) ->
  error_logger:warning_msg ("~p : Unrecognized call ~p from ~p~n",
                            [?MODULE, Request, From]),
  { reply, ok, State }.

handle_cast (Request, State) ->
  error_logger:warning_msg ("~p : Unrecognized cast ~p~n",[?MODULE, Request]),
  { noreply, State }.

handle_info (Request, State) ->
  error_logger:warning_msg ("~p : Unrecognized info ~p~n",[?MODULE, Request]),
  { noreply, State }.

terminate (_Reason, #state { file = _File }) ->
  ok.

code_change (_OldVsn, State, _Extra) ->
  { ok, State }.

%%====================================================================
%% internal functions
%%====================================================================
get_dirs () ->
  mondemand_global:get (md_be_rrd_dirs).

check_error_timeout (Timestamp, CurrentTimestamp) ->
  %% True if error timeout elapsed, False otherwise
  mondemand_util:now_to_epoch_secs(CurrentTimestamp)
    - mondemand_util:now_to_epoch_secs(Timestamp)
  >= mondemand_global:get(error_timeout).

check_error_duration (Key, Error, Timestamp, CurrentTimestamp) ->
  case check_error_timeout(Timestamp, CurrentTimestamp) of
    true -> clear_entry (Key),
            {error, clearing, Key};
    false -> {error, Error, Key}
  end.

filename_to_key (Key) when is_tuple (Key) ->
  Key;
filename_to_key (Filename) when is_list (Filename) ->
  filename_to_key (list_to_binary (Filename));
filename_to_key (Filename) when is_binary (Filename) ->
  case ets:select(?TABLE,
                  ets:fun2ms(fun({K,F,_,_}) when F =:= Filename -> K end)) of
    [] -> undefined;
    [O] -> O
  end.

update_state_and_timestamp (Key, State) when is_tuple (Key) ->
  ets:update_element (?TABLE, Key, [{3, State}, {4, os:timestamp ()}]);
update_state_and_timestamp (Filename, State) ->
  case filename_to_key (Filename) of
      undefined -> false;
      Key -> update_state_and_timestamp (Key, State)
  end.

update_state (Key, State) when is_tuple (Key) ->
  ets:update_element (?TABLE, Key, {3, State});
update_state (Filename, State) ->
  case filename_to_key (Filename) of
    undefined -> false;
    Key -> update_state (Key, State)
  end.

save_cache (File) ->
  error_logger:info_msg ("saving file name cache to ~p",[File]),
  PreProcess = os:timestamp (),
  Result = ets:tab2file (?TABLE, File),
  PostProcess = os:timestamp (),
  ProcessMillis = webmachine_util:now_diff_milliseconds (PostProcess, PreProcess),
  error_logger:info_msg ("saved file name cache to ~p in ~p millis",
                         [File, ProcessMillis]),
  Result.

load_cache (File) ->
  error_logger:info_msg ("loading file name cache from ~p",[File]),
  PreProcess = os:timestamp (),
  Result =
    case ets:file2tab (File) of
      {ok, ?TABLE} ->
        % in some cases if the server is restarted after a cache file has been
        % written but while the builder is still building, cache entries can
        % be left in the creating state, and so will never receive updates, so
        % on startup, just remove all those entries
        error_logger:info_msg ("start clearing inflight data from cache"),
        ets:select_delete (?TABLE,
                           ets:fun2ms(fun({_,_,creating,_}) -> true end)),
        error_logger:info_msg ("done clearing inflight data from cache"),
        true;
      % FIXME: temporary for upgrade from pre-1.0.0
      {ok, ?OLD_TABLE} ->
        ets:rename (?OLD_TABLE, ?TABLE),
        true;
      Error ->
        error_logger:error_msg (
          "failed to load file name cache from ~p because ~p",[File, Error]),
        false
    end,
  PostProcess = os:timestamp (),
  ProcessMillis =
    webmachine_util:now_diff_milliseconds (PostProcess, PreProcess),
  error_logger:info_msg ("loaded file name cache from ~p in ~p millis",
                         [File, ProcessMillis]),
  Result.

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


-endif.
