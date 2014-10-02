-module (mondemand_backend_stats_rrd_filecache).

-behaviour (gen_server).

%% API
-export ([ start_link/1,
           check_cache/6
         ]).

%% gen_server callbacks
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3
         ]).

-record (state, { delay, interval, file }).
-define (TABLE, md_be_stats_rrd_filecache).

start_link (FileNameCacheFile) ->
  gen_server:start_link ({local, ?MODULE},?MODULE,[[FileNameCacheFile]],[]).

save_cache (File) ->
  error_logger:info_msg ("saving file name cache to ~p",[File]),
  ets:tab2file (?TABLE, File).

load_cache (File) ->
  case ets:file2tab (File) of
    {ok, ?TABLE} -> true;
    _ -> false
  end.

%%====================================================================
%% gen_server callbacks
%%====================================================================
init ([FileNameCacheFile]) ->
  Interval = 3600,         % flush each hour
  Delay = Interval * 1000, % delay is in milliseconds

  % so our terminate/2 always gets called
  process_flag( trap_exit, true ),

  % attempt to load the file cache from a file at startup
  case load_cache (FileNameCacheFile) of
    true -> ok;
    false ->
      % it it failed we'll recreate it
      ets:new (?TABLE, [set, public, named_table, {keypos, 1}])
  end,

  { ok,
    #state { delay = Delay, interval = Interval, file = FileNameCacheFile },
    0 % cause cache refresh to happen on startup
  }.

handle_call (Request, From, State = #state { delay = Delay }) ->
  error_logger:warning_msg ("~p : Unrecognized call ~p from ~p~n",
                            [?MODULE, Request, From]),
  { reply, ok, State, Delay }.

handle_cast (Request, State = #state { delay = Delay }) ->
  error_logger:warning_msg ("~p : Unrecognized cast ~p~n",[?MODULE, Request]),
  { noreply, State, Delay }.

handle_info (timeout, State = #state { delay = Delay, file = File }) ->
  update_cache (File),
  { noreply, State, Delay };
handle_info (Request, State = #state { delay = Delay }) ->
  error_logger:warning_msg ("~p : Unrecognized info ~p~n",[?MODULE, Request]),
  { noreply, State, Delay }.

terminate (_Reason, #state { file = File }) ->
  % save a copy to disk
  save_cache (File),
  ok.

code_change (_OldVsn, State, _Extra) ->
  { ok, State }.

%% API
check_cache (Prefix, ProgId, MetricType, MetricName, Host, Context) ->
  FileKey = {ProgId,MetricType,MetricName,Host,Context},
  case ets:lookup (?TABLE, FileKey) of
    [{_, FP}] ->
      {ok, FP};
    [] ->
      ContextString =
        case Context of
          [] -> "";
          L -> [ "-",
                 mondemand_server_util:join ([[K,"=",V] || {K, V} <- L ], "-")
               ]
        end,

      FileName = list_to_binary ([ProgId,
                                  "-",MetricType,
                                  "-",MetricName,
                                  "-",Host,
                                  ContextString,
                                  ".rrd"]),
      FilePath =
        filename:join([Prefix,
                       ProgId,
                       MetricName]),

      % TODO: check for error and don't create if it's there
      mondemand_server_util:mkdir_p (FilePath),

      RRDFile = filename:join ([FilePath, FileName]),
      case maybe_create (MetricType, RRDFile) of
        {ok, _} ->
          ets:insert (?TABLE, {FileKey, RRDFile}),
          {ok, RRDFile};
        {timeout, Timeout} ->
          error_logger:error_msg (
            "Unable to create '~p' because of timeout ~p",[RRDFile, Timeout]),
          error;
        {error, Error} ->
          error_logger:error_msg (
            "Unable to create '~p' because of ~p",[RRDFile, Error]),
          error;
        Unknown ->
          error_logger:error_msg (
            "Unable to create '~p' because of unknown ~p",[RRDFile, Unknown]),
          error
      end
  end.

update_cache (FileNameCacheFile) ->
  error_logger:info_msg ("Flushing File Name Cache"),
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
  % save a copy to disk after each flush
  save_cache (FileNameCacheFile),
  ok.

maybe_create (Type, File) ->
  case file:read_file_info (File) of
    {ok, I} -> {ok, I};
    _ ->
      case Type of
        <<"counter">> -> create_counter (File);
        <<"gauge">> -> create_gauge (File);
        _ -> create_counter (File) % default is counter
      end
  end.

create_counter (File) ->
  erlrrd:create ([
      io_lib:fwrite ("~s",[File]),
      " --step \"60\""
      " --start \"now - 90 days\""
      " \"DS:value:DERIVE:900:0:U\""
      " \"RRA:AVERAGE:0.5:1:44640\""
      " \"RRA:AVERAGE:0.5:15:9600\""
      " \"RRA:AVERAGE:0.5:1440:400\""
    ]).

create_gauge (File) ->
  erlrrd:create ([
      io_lib:fwrite ("~s",[File]),
      " --step \"60\""
      " --start \"now - 90 days\""
      " \"DS:value:GAUGE:900:U:U\""
      " \"RRA:AVERAGE:0.5:1:44640\""
      " \"RRA:AVERAGE:0.5:15:9600\""
      " \"RRA:AVERAGE:0.5:1440:400\""
    ]).


