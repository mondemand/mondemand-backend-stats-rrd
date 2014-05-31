-module (mondemand_backend_stats_rrd_resource).

-export ([ init/1 ]).
-export ([ allowed_methods/2,
           resource_exists/2,
           content_types_provided/2,
           to_json/2,
           to_javascript/2
         ]).

-record (state, {root, type, prog_id, metric, file}).

-include_lib ("kernel/include/file.hrl").
-include_lib ("webmachine/include/webmachine.hrl").

init (Config) ->
  {root, Root} = proplists:lookup (root, Config),
  {ok, #state { root = Root } }.

allowed_methods (ReqData, State) ->
  {['GET'], ReqData, State}.

resource_exists(ReqData, State = #state { root = Root }) ->
  % because we have a dispatch rules of
  %   {["rrd",prog_id,metric_name], ...}
  %   {["rrd",prog_id], ...}
  %   [{"rrd"], ...}
  % we'll check the path info for the three cases
  PathInfo = wrq:path_info (ReqData),
  case PathInfo of
    [] ->
      check_root_dir (ReqData, State, Root);
    L when length (L) =:= 1 ->
      check_prog_id_dir (ReqData, State, Root, L);
    L when length (L) =:= 2 ->
      check_prog_id_metric_dir (ReqData, State, Root, L);
    L when length (L) =:= 3 ->
      check_rrd_file (ReqData, State, Root, L);
    _ ->
      {false, ReqData, State}
  end.

check_root_dir (ReqData, State, Root) ->
  case filelib:is_dir (Root) of
    true -> {true, ReqData, State#state { type = prog_id, file = Root}};
    false -> {false, ReqData, State}
  end.

check_prog_id_dir (ReqData, State, Root, [{prog_id, ProgId}]) ->
  FilePath = filename:join ([Root, ProgId]),
  case filelib:is_dir (FilePath) of
    true -> {true, ReqData, State#state { type = metrics, prog_id = ProgId,
                                          file = FilePath }};
    false -> {false, ReqData, State}
  end.

check_prog_id_metric_dir (ReqData, State, Root, L) ->
  case { proplists:get_value (prog_id, L),
         proplists:get_value (metric, L) } of
    { ProgId, Metric } when ProgId =/= undefined; Metric =/= undefined ->
      FilePath = filename:join ([Root, ProgId, Metric]),
      case filelib:is_dir (FilePath) of
        true -> {true, ReqData,
                 State#state { type = rrds,
                               prog_id = ProgId,
                               metric = Metric,
                               file = FilePath }};
        false -> {false, ReqData, State}
      end;
    _ ->
      {false, ReqData, State}
  end.

check_rrd_file (ReqData, State, Root, L) ->
  case { proplists:get_value (prog_id, L),
         proplists:get_value (metric, L),
         proplists:get_value (file, L)
       } of
    { ProgId, Metric, File }
      when ProgId =/= undefined; Metric =/= undefined ; File =/= undefined ->
        FilePath = filename:join ([Root, ProgId, Metric, File]),
        case filelib:is_regular (FilePath) of
          true -> {true, ReqData,
                   State#state { type = rrd,
                                 prog_id = ProgId,
                                 metric = Metric,
                                 file = FilePath } };
          false -> {false, ReqData, State}
        end;
      _ ->
        {false, ReqData, State}
  end.

% order matters here, if you put text/javascript first, you will get a 500
% as it will use that if a content type is not specified and it will require
% a jsonp query arg, if we list application/json first then hitting this
% with a browser and without jsonp will work
content_types_provided (ReqData, State) ->
  { [ {"application/json", to_json},
      {"text/javascript", to_javascript}
    ],
    ReqData, State
  }.

to_javascript (ReqData, State) ->
  case get_jsonp (ReqData) of
    undefined ->
      % in order to get javascript you need to specify a callback, otherwise
      % it's considered an error
      { { halt, 500}, ReqData, State };
    _ ->
      to_json (ReqData, State)
  end.

to_json (ReqData,
         State = #state { type = Type, prog_id = _ProgId,
                          metric = _Metric, file = FilePath }) ->

  Prefix = get_prefix (ReqData),

  JSON =
    case Type of
      prog_id -> % list prog_ids
        list_prog_ids (Prefix, FilePath);
      metrics -> % list metric for prog_id
        list_metrics (Prefix, FilePath);
      rrds -> % list rrds for prog_id and metric
        list_rrds (Prefix, FilePath);
      rrd -> % return file for owner and id and name
        get_file (FilePath)
    end,

  ResponseBody =
    case get_jsonp (ReqData) of
      undefined -> JSON;
      Callback -> [ Callback, "(", JSON, ");" ]
    end,

  { ResponseBody, ReqData, State }.

list_prog_ids (Prefix, Root) ->
  list_files (Prefix, Root).

list_metrics (Prefix, Root) ->
  list_files (Prefix, Root).

list_rrds (Prefix, Root) ->
  list_files (Prefix, Root).

list_files (Prefix, Dir) ->
  AllFiles =
    case file:list_dir (Dir) of
      {ok, F} -> F;
      {error, _} -> []
    end,
  FilteredByPrefix =
    case Prefix of
      undefined -> AllFiles;
      _ ->
        lists:filter (
          fun (E) -> lists:prefix (Prefix, E) end,
          AllFiles)
    end,
  mochijson2:encode ([ [{ <<"label">>, list_to_binary (F)}]
                       || F <- lists:sort (FilteredByPrefix) ]).

get_file (FilePath) ->
  case file:read_file (FilePath) of
    {ok, Bin} -> Bin;
    _ -> <<"{}">>  % return empty json for error
  end.

get_jsonp (ReqData) ->
  case wrq:get_qs_value ("jsonp", ReqData) of
    undefined -> wrq:get_qs_value ("callback", ReqData);
    V -> V
  end.

get_prefix (ReqData) ->
  wrq:get_qs_value ("term", ReqData).
