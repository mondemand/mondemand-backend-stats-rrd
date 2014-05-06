-module (mondemand_backend_stats_rrd).

-behaviour (gen_server).
-behaviour (mondemand_server_backend).
-behaviour (mondemand_backend_stats_handler).

-export ([ update_cache/0 ]).

%% mondemand_server_backend callbacks
-export ([ start_link/1,
           process/1,
           stats/0,
           required_apps/0
         ]).

%% mondemand_backend_stats_handler callbacks
-export ([ header/0,
           separator/0,
           format_stat/8,
           footer/0,
           handle_response/1
         ]).

%% gen_server callbacks
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3
         ]).

-record (state, { sidejob,
                  stats = dict:new ()
                }).
-define (TABLE, md_be_stats_rrd_filecache).

%%====================================================================
%% mondemand_server_backend callbacks
%%====================================================================
start_link (Config) ->
  gen_server:start_link ( { local, ?MODULE }, ?MODULE, Config, []).

process (Event) ->
  mondemand_backend_connection_pool:cast (?MODULE, {process, Event}).

stats () ->
  gen_server:call (?MODULE, {stats}).

required_apps () ->
  [ sidejob, afunix, erlrrd ].

%%====================================================================
%% gen_server callbacks
%%====================================================================
init (Config) ->
  Limit = proplists:get_value (limit, Config, 10),
  Number = proplists:get_value (number, Config, undefined),
  Prefix = proplists:get_value (prefix, Config, "."),

  mondemand_server_util:mkdir_p (Prefix),

  ets:new (?TABLE, [set, public, named_table, {keypos, 1}]),

  { ok, Proc } =
    mondemand_backend_connection_pool:init ([?MODULE, Limit, Number]),
  { ok, #state { sidejob = Proc } }.

handle_call ({stats}, _From, State) ->
  Stats = mondemand_backend_connection_pool:stats (?MODULE),
  { reply, Stats, State };
handle_call (Request, From, State) ->
  error_logger:warning_msg ("~p : Unrecognized call ~p from ~p~n",
                            [?MODULE, Request, From]),
  { reply, ok, State }.

handle_cast (Request, State) ->
  error_logger:warning_msg ("~p : Unrecognized cast ~p~n",[?MODULE, Request]),
  { noreply, State }.

handle_info (Request, State) ->
  error_logger:warning_msg ("~p : Unrecognized info ~p~n",[?MODULE, Request]),
  {noreply, State}.

terminate (_Reason, #state { }) ->
  ok.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

%%====================================================================
%% mondemand_backend_stats_handler callbacks
%%====================================================================
header () -> "BATCH\n".

separator () -> "\n".

format_stat (Prefix, ProgId, Host,
             MetricType, MetricName, MetricValue, Timestamp, Context) ->

  RRDFilePath =
    check_cache (Prefix,ProgId,MetricType,MetricName,Host,Context),
  case RRDFilePath of
    error -> error;
    {ok, P} ->
      [ "UPDATE ",P, io_lib:fwrite (" ~b:~b", [Timestamp,MetricValue])]
  end.

footer () -> "\n.\n".

handle_response (Response) ->
  parse_response (Response).

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

      mondemand_server_util:mkdir_p (FilePath),

      RRDFile = filename:join ([FilePath, FileName]),
      case maybe_create (MetricType, RRDFile) of
        {ok, _} ->
          ets:insert (?TABLE, {FileKey, RRDFile}),
          {ok, RRDFile};
        {error, Error} ->
          error_logger:error_msg (
            "Unable to create '~p' because of ~p",[RRDFile, Error]),
          error
      end
  end.

update_cache () ->
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

parse_response (Response) ->
  Lines = string:tokens (Response, "\n"),
  Result = parse_lines (Lines,0),
  case Result =/= 0 of
    true ->
      error_logger:error_msg ("~p : got errors ~p",[?MODULE, Result]);
    false ->
      ok
  end,
  Result.

parse_lines ([],Accum) ->
  Accum;
parse_lines ([Status|Rest],Accum) ->
  Resp =
    case parse_status_line (Status) of
      {status, N, StatusMessage} when N < 0 ->
        parse_status_message (StatusMessage);
      {status, N, errors} when N > 0 ->
        {ok, N, parse_rest (N, Rest)};
      E ->
        E
    end,
  case Resp of
    {error, _} ->
      parse_lines (Rest, Accum + 1);
    {ok, Errs, RealRest} ->
      parse_lines (RealRest, Accum + Errs);
    _ ->
      parse_lines (Rest, Accum)
  end.

parse_status_message ("No such file: " ++ File) ->
  {error, {no_file, File}};
parse_status_message (Line) ->
  {error, Line}.

parse_rest (0, Rest) ->
  Rest;
parse_rest (N, [_|Rest]) ->
  parse_rest (N - 1, Rest).

parse_status_line (Status) ->
  case string:to_integer (Status) of
    {error, E} -> {error, {parse_status_line, E}};
    {N, [32|R]} -> case R of
                     "errors" -> {status, N, errors};
                     _ -> {status, N, R}
                   end 
  end.


