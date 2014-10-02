-module (mondemand_backend_stats_rrd).

-behaviour (supervisor).
-behaviour (mondemand_server_backend).
-behaviour (mondemand_backend_stats_handler).

%% mondemand_server_backend callbacks
-export ([ start_link/1,
           process/1,
           required_apps/0,
           type/0
         ]).

%% mondemand_backend_stats_handler callbacks
-export ([ header/0,
           separator/0,
           format_stat/8,
           footer/0,
           handle_response/2
         ]).

%% supervisor callbacks
-export ([init/1]).

%%====================================================================
%% mondemand_server_backend callbacks
%%====================================================================
start_link (Config) ->
  supervisor:start_link ({local, ?MODULE}, ?MODULE, [Config]).

process (Event) ->
  mondemand_backend_worker_pool_sup:process
    (mondemand_backend_stats_rrd_worker_pool, Event).

required_apps () ->
  [ erlrrd ].

type () ->
  supervisor.

%%====================================================================
%% supervisor callbacks
%%====================================================================
init ([Config]) ->
  Prefix = proplists:get_value (prefix, Config, "."),

  mondemand_server_util:mkdir_p (Prefix),

  Number = proplists:get_value (number, Config, 16), % FIXME: replace default
  FileNameCache =
    proplists:get_value (file_cache, Config, "/tmp/file_name_cache.ets"),

  { ok,
    {
      {one_for_one, 10, 10},
      [
        { mondemand_backend_stats_rrd_filecache,
          { mondemand_backend_stats_rrd_filecache, start_link,
            [FileNameCache]
          },
          permanent,
          2000,
          worker,
          [ mondemand_backend_stats_rrd_filecache ]
        },
        { mondemand_backend_stats_rrd_worker_pool,
          { mondemand_backend_worker_pool_sup, start_link,
            [ mondemand_backend_stats_rrd_worker_pool,
              mondemand_backend_connection,
              Number,
              ?MODULE ]
          },
          permanent,
          2000,
          supervisor,
          [ ]
        }
      ]
    }
  }.

%%====================================================================
%% mondemand_backend_stats_handler callbacks
%%====================================================================
header () -> "BATCH\n".

separator () -> "\n".

format_stat (Prefix, ProgId, Host,
             MetricType, MetricName, MetricValue, Timestamp, Context) ->

  RRDFilePath =
    mondemand_backend_stats_rrd_filecache:check_cache
      (Prefix,ProgId,MetricType,MetricName,Host,Context),
  case RRDFilePath of
    error -> error;
    {ok, P} ->
      [ "UPDATE ",P, io_lib:fwrite (" ~b:~b", [Timestamp,MetricValue])]
  end.

footer () -> "\n.\n".

handle_response (Response, Previous) ->
  parse_response (Response, Previous).


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
