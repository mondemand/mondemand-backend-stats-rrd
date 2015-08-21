-module (mondemand_backend_stats_rrd_filecache).

-behaviour (gen_server).
-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export ([ start_link/3,
           get_dirs/0,
           check_cache/6,
           clear_path/1,
           save_cache/0,
           update_cache/0
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
    ets:foldl (fun ({Key,File}, Accum) ->
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

handle_cast ({clear_path, Path}, State) when is_list (Path) ->
  handle_cast ({clear_path, list_to_binary(Path)}, State);
handle_cast ({clear_path, Path}, State) ->
  ets:select_delete(?TABLE,
    ets:fun2ms(fun({{_,_,_,_,_},F}) when F =:= Path -> true end)
  ),
  { noreply, State};
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

clear_path (Path) ->
  gen_server:cast (?MODULE, {clear_path, Path}).

%% API
check_cache (Prefix, ProgIdIn, MetricType,
             MetricNameIn, HostIn, ContextIn) ->
  FileKey = {ProgIdIn, MetricType, MetricNameIn, HostIn, ContextIn},
  case ets:lookup (?TABLE, FileKey) of
    [{_, FP}] ->
      {ok, FP};
    [] ->
      {FullyQualifiedPrefix, ProgId, MetricName, Host, Context,
       AggregatedType, FilePath, RRDFile} =
        mondemand_backend_stats_rrd_builder:rrdfilename
          (Prefix, ProgIdIn, MetricType, MetricNameIn, HostIn, ContextIn),
      mondemand_backend_stats_rrd_builder:build (FullyQualifiedPrefix,
        ProgId, MetricType, MetricName, Host, Context, AggregatedType,
        FilePath, RRDFile),
      ets:insert (?TABLE, {FileKey, RRDFile}),
      {ok, RRDFile}
  end.
