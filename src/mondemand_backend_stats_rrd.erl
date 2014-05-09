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
           handle_response/2
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
  case mondemand_backend_connection_pool:cast (?MODULE, {process, Event}) of
    overload ->
      error_logger:info_msg ("overloaded~n",[]),
      overload;
    O ->
      O
  end.

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

handle_response (Response, Previous) ->
  parse_response (Response, Previous).

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


-record (parse_state, {errors = 0, expected = 0, remaining}).

parse_response (Response, undefined) ->
  parse_response (Response, #parse_state {});
parse_response (Response, State = #parse_state { remaining = Remaining }) ->
  StateOut =
    case Remaining of
      undefined ->
        get_lines (Response, State);
      _ ->
        get_lines (Remaining ++ Response,
                   State#parse_state { remaining = undefined })
    end,
  { StateOut#parse_state.errors, StateOut#parse_state { errors = 0 } }.

get_lines (Lines, State = #parse_state { errors = CurrentErrors,
                                         expected = Expected }) ->
  case get_line (Lines) of
    {undefined, ""} ->
      State#parse_state { remaining = undefined };
    {undefined, Rest} ->
      State#parse_state { remaining = Rest };
    {Line, Rest} ->
      { Errors, Expecting } =
        case process_line (Line) of
          {error, Message} ->
            error_logger:error_msg ("~p",[Message]),
            { 1, 0 };
          {expecting, N} ->
            { N, N };
          ok ->
            { 0, 0 }
        end,
      ExpectedOut =
        case Expected =/= 0 of
          true ->
            error_logger:error_msg ("~p",[Line]),
            Expected - 1;
          false ->
            Expecting
        end,
      get_lines (Rest, State#parse_state { errors = CurrentErrors + Errors,
                                           expected = ExpectedOut })
  end.

get_line (Lines) ->
  get_line (Lines, []).

get_line ([], CurrentLine) ->
  { undefined, lists:reverse (CurrentLine) };
get_line ([$\n|Rest], CurrentLine) ->
  {lists:reverse (CurrentLine), Rest};
get_line ([Char|Remaining], CurrentLine) ->
  get_line (Remaining, [Char|CurrentLine]).

process_line (Line) ->
  case parse_status_line (Line) of
    {status, N, StatusMessage} when N < 0 ->
      {error, parse_status_message (StatusMessage)};
    {status, N, errors} when N > 0 ->
      {expecting, N};
    _ ->
      ok
  end.

parse_status_line (Status) ->
  case string:to_integer (Status) of
    {error, E} -> {error, {parse_status_line, E}};
    {N, [32|R]} ->
      case R of
        "errors" -> {status, N, errors};
        _ -> {status, N, R}
      end
  end.

parse_status_message ("No such file: " ++ File) ->
  {error, {no_file, File}};
parse_status_message (Line) ->
  {error, Line}.
