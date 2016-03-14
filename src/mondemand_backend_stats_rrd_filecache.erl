-module (mondemand_backend_stats_rrd_filecache).

-behaviour (gen_server).
-include_lib("kernel/include/file.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export ([ start_link/3,
           get_dirs/0,
           check_cache/6,
           key/5,
           clear_path/1,
           delete_key/1,
           save_cache/0,
           update_cache/0,
           clear_errors/0,
           show_errors/0,
           mark_error/2,
           mark_created/1,
           filename_to_key/1,
           migrate_cache/1,
           migrate_one/1
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
-define (TABLE, md_be_stats_rrd_filecache).

start_link (FileNameCacheFile, HostDir, AggregateDir) ->
  mondemand_global:put (md_be_rrd_dirs, {HostDir, AggregateDir}),
  gen_server:start_link ({local, ?MODULE},?MODULE,
                         [FileNameCacheFile, HostDir, AggregateDir],[]).

get_dirs () ->
  mondemand_global:get (md_be_rrd_dirs).

save_cache () ->
  File = gen_server:call(?MODULE,{cache_file}),
  save_cache (File).

save_cache (File) ->
  error_logger:info_msg ("saving file name cache to ~p",[File]),
  PreProcess = os:timestamp (),
  Result = ets:tab2file (?TABLE, File),
  PostProcess = os:timestamp (),
  ProcessMillis =
    webmachine_util:now_diff_milliseconds (PostProcess, PreProcess),
  error_logger:info_msg ("saved file name cache to ~p in ~p millis",[File, ProcessMillis]),
  Result.

migrate_cache (MillisPause) ->
  File = gen_server:call(?MODULE,{cache_file}),
  PreProcess = os:timestamp (),
  migrate_all (MillisPause),
  PostProcess = os:timestamp (),
  ProcessMillis =
    webmachine_util:now_diff_milliseconds (PostProcess, PreProcess),
  error_logger:info_msg ("migrated file name cache (~p) in ~p millis",
                         [File, ProcessMillis]),
  ok.

migrate_all (MillisPause) ->
  First = ets:first (?TABLE),
  migrate_one (First),
  migate_until_end (MillisPause, First).

migate_until_end (MillisPause, PrevKey) ->
  case ets:next (?TABLE, PrevKey) of
    '$end_of_table' -> ok;
    Key ->
      timer:sleep (MillisPause),
      migrate_one (Key),
      migate_until_end (MillisPause, Key)
  end.

mdyhms_to_epoch_seconds (DateTime) ->
  EpochStartSeconds =
    calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
  DateTimeSeconds =
    calendar:datetime_to_gregorian_seconds(DateTime),
  DateTimeSeconds - EpochStartSeconds.

file_mtime_in_seconds (File) ->
  case file:read_file_info (File) of
    {ok, #file_info { mtime = Mtime } } ->
       {ok, mdyhms_to_epoch_seconds (Mtime)};
    E -> E
  end.

migrate_one (Key) ->
  case ets:lookup (?TABLE, Key) of
    [{_,F}] ->
      case file_mtime_in_seconds (F) of
        {ok, Mtime} ->
          ets:insert (?TABLE, {Key, F, created, Mtime}),
          ok;
        {error, E} ->
          ets:insert (?TABLE, {Key, F, {error, E}, undefined}),
          ok
      end;
    _ ->
      ok
  end.

show_errors () ->
  ets:select (?TABLE,
              ets:fun2ms(fun({K,_,{error,E},_}) -> {K,E} end)).

clear_errors () ->
  ets:select_delete (?TABLE,
                     ets:fun2ms (fun({_,_,{error,_},_}) -> true end)).

clear_path (Path) when is_list (Path) ->
  clear_path (list_to_binary (Path));
clear_path (Path) ->
  ets:select_delete(?TABLE,
    ets:fun2ms(fun({{_,_,_,_,_},F}) when F =:= Path -> true;
                  ({{_,_,_,_,_},F,_,_}) when F =:= Path -> true
               end)
  ).

delete_key (Key) when is_tuple (Key) ->
  ets:delete (?TABLE, Key).

mark_error (FilenameOrKey, Error)  ->
  error_logger:error_msg ("Failure ~p for Key ~p Blackholing",
                          [Error, filename_to_key (FilenameOrKey)]),
  update_state (FilenameOrKey, {error, Error}).
mark_created (FilenameOrKey) ->
  update_state (FilenameOrKey, created).

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

update_state (Key, State) when is_tuple (Key) ->
  ets:update_element (?TABLE, Key, {3, State});
update_state (Filename, State) ->
  case filename_to_key (Filename) of
    undefined -> false;
    Key -> update_state (Key, State)
  end.

update_cache () ->
  File = gen_server:call(?MODULE,{cache_file}),
  update_cache (File).

update_cache (FileNameCacheFile) ->
  error_logger:info_msg ("updating file name cache to ~p",[FileNameCacheFile]),
  PreProcess = os:timestamp (),
  % sometimes RRD's will be deleting if they are no longer being accessed,
  % in those cases we want to remove it from the cache, so this will do
  % that by maybe recreating it
  ToDelete =
    ets:foldl (fun
                 ({Key,File}, Accum) ->
                   case file:read_file_info (File) of
                     {ok, _} -> Accum;  % already exists so keep it
                     _ -> [Key|Accum]   % doesn't so add to the to delete list
                   end;
                 ({Key, File, _State, _Time}, Accum) ->
                   case file:read_file_info (File) of
                     {ok, _} -> Accum;  % already exists so keep it
                     _ -> [Key|Accum]   % doesn't so add to the to delete list
                   end
               end,
               [],
               ?TABLE),
  % actually peform the deletes
  [ ets:delete (?TABLE, K) || K <- ToDelete ],
  PostProcess = os:timestamp (),
  ProcessMillis =
    webmachine_util:now_diff_milliseconds (PostProcess, PreProcess),
  error_logger:info_msg ("updated file name cache to ~p in ~p millis",
                         [FileNameCacheFile, ProcessMillis]),

  % save a copy to disk after each flush
  save_cache (FileNameCacheFile),
  ok.

load_cache (File) ->
  error_logger:info_msg ("loading file name cache from ~p",[File]),
  PreProcess = os:timestamp (),
  Result =
    case ets:file2tab (File) of
      {ok, ?TABLE} -> true;
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


key (ProgId, MetricType, MetricName, Host, Context) ->
  {ProgId, MetricType, MetricName, Host, Context}.

%% API
%% return {ok, FilePath, FileKey} or {error, Reason, FileKey}
check_cache (Prefix, ProgIdIn, MetricType,
             MetricNameIn, HostIn, ContextIn) ->
  FileKey = key (ProgIdIn, MetricType, MetricNameIn, HostIn, ContextIn),
  case ets:lookup (?TABLE, FileKey) of
    [{_, FP, created, _}] ->
      {ok, FP, FileKey};
    [{_, _, State, _}] ->
      case State of
        creating -> {error, creating, FileKey};
        clearing -> {error, clearing, FileKey};
        {error, E} -> {error, E, FileKey};
        error -> {error, error, FileKey}
      end;
    [{_, FP}] ->
      {ok, FP, FileKey};
    [] ->
      {FullyQualifiedPrefix, ProgId, MetricName, Host, Context,
       AggregatedType, FilePath, RRDFile} =
        mondemand_backend_stats_rrd_builder:rrdfilename
          (Prefix, ProgIdIn, MetricType, MetricNameIn, HostIn, ContextIn),
      % mark as being created
      ets:insert (?TABLE, {FileKey, RRDFile, creating, os:timestamp()}),
      % then create
      mondemand_backend_stats_rrd_builder:build (FullyQualifiedPrefix,
        ProgId, MetricType, MetricName, Host, Context, AggregatedType,
        FilePath, RRDFile),
      % and return to caller that we are creating so no writes are attempted
      % until it's created
      {error, creating, FileKey}
  end.
