-module (mondemand_backend_stats_rrd_builder).

-behaviour (gen_server).

%% API
-export ([ start_link/0,
           build/9,
           rrdfilename/6
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

start_link () ->
  gen_server:start_link ({local, ?MODULE},?MODULE, [], []).

build (FullyQualifiedPrefix, ProgId, MetricType,
       MetricName, Host, Context, AggregatedType,
       FilePath, RRDFile) ->
  gen_server:cast (?MODULE,
    {build, FullyQualifiedPrefix, ProgId, MetricType,
            MetricName, Host, Context, AggregatedType,
            FilePath, RRDFile}).

rrdfilename (Prefix, ProgIdIn, MetricType, MetricNameIn, HostIn, ContextIn) ->
  % normalize to binary to simplify rest of code
  ProgId = mondemand_util:binaryify (ProgIdIn),
  MetricName = mondemand_util:binaryify (MetricNameIn),
  Host = mondemand_util:binaryify (HostIn),
  Context = [ {mondemand_util:binaryify(K), mondemand_util:binaryify(V) }
              || {K, V} <- ContextIn ],

  % all aggregated values end up with a <<"stat">> context value, so
  % remove it and get the type
  AggregatedType =
    case lists:keyfind (<<"stat">>,1,Context) of
      false -> undefined;
      {_,AT} -> AT
    end,

  % this should have no impact on systems which use absolute paths
  % but for those which use relative paths this will make sure
  % symlinks (and maybe soon hardlinks), work.  Mostly this is
  % for development
  FullyQualifiedPrefix =
    case Prefix of
      [$/ | _ ] -> Prefix;
      _ ->
        {ok, CWD} = file:get_cwd(),
        filename:join ([CWD, Prefix])
    end,

  % generate some paths and files which graphite understands
  {FilePath, FileName} =
     legacy_rrd_path (FullyQualifiedPrefix, ProgId, MetricType,
                      MetricName, Host, Context),

  RRDFile = filename:join ([FilePath, FileName]),
  {FullyQualifiedPrefix, ProgId, MetricName, Host,
   Context, AggregatedType, FilePath, RRDFile}.

%%====================================================================
%% gen_server callbacks
%%====================================================================
init ([]) ->
  { ok, #state {} }.

handle_call (Request, From, State = #state {}) ->
  error_logger:warning_msg ("~p : Unrecognized call ~p from ~p~n",
                            [?MODULE, Request, From]),
  { reply, ok, State }.

handle_cast ({build,FullyQualifiedPrefix, ProgId, MetricType,
                    MetricName, Host, Context, AggregatedType,
                    FilePath, RRDFile},
             State = #state {}) ->
  ibuild(FullyQualifiedPrefix, ProgId, MetricType,
         MetricName, Host, Context, AggregatedType, FilePath, RRDFile),
  {noreply, State};
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

% internal functions
legacy_rrd_path (Prefix, ProgId,
                   {statset, SubType}, MetricName, Host, Context) ->
  legacy_rrd_path (Prefix, ProgId, SubType, MetricName, Host, Context);
legacy_rrd_path (Prefix, ProgId, MetricType, MetricName, Host, Context) ->
  ContextString =
    case Context of
      [] -> "";
      L -> [ "-",
             mondemand_server_util:join ([[K,"=",V] || {K, V} <- L ], "-")
           ]
    end,

  FileName = list_to_binary ([ProgId,
                              "-",atom_to_list (MetricType),
                              "-",MetricName,
                              "-",Host,
                              ContextString,
                              ".rrd"]),
  FilePath =
    filename:join([Prefix,
                   ProgId,
                   MetricName]),

  {FilePath, FileName}.

ibuild (FullyQualifiedPrefix, ProgId, MetricType,
        MetricName, Host, Context, AggregatedType, FilePath, RRDFile) ->

  case mondemand_server_util:mkdir_p (FilePath) of
    ok -> ok;
    E2 ->
      error_logger:error_msg (
        "Can't create dir ~p: ~p",[FilePath, E2])
  end,

  case maybe_create (MetricType, AggregatedType, RRDFile) of
    {ok, _} ->
      {GraphitePath, GraphiteFile} =
        graphite_rrd_path (FullyQualifiedPrefix, ProgId, MetricType,
                           MetricName, Host, Context,
                           AggregatedType =/= undefined),
      GRRDFile = filename:join ([GraphitePath, GraphiteFile]),

      % TODO: check for error and don't create if it's there
      case mondemand_server_util:mkdir_p (GraphitePath) of
        ok -> ok;
        E ->
          error_logger:error_msg (
            "Can't create graphite dir ~p: ~p",[GraphitePath, E])
      end,
      % making symlinks for the moment
      file:make_symlink (RRDFile, GRRDFile),
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
  end.

graphite_normalize_host (Host) ->
  case re:run(Host,"([^\.]+)", [{capture, all_but_first, binary}]) of
    {match, [H]} -> H;
    nomatch -> Host
  end.

graphite_normalize_token (Token) ->
  re:replace (Token, "\\W", "_", [global, {return, binary}]).


graphite_rrd_path (Prefix, ProgId,
                   {statset, SubType}, MetricName, Host, Context,
                   IsAggregate) ->
  graphite_rrd_path (Prefix, ProgId, SubType, MetricName, Host, Context,
                     IsAggregate);
graphite_rrd_path (Prefix, ProgId,  MetricType, MetricName, Host, Context,
                   IsAggregate) ->
  ContextParts =
    case Context of
      [] -> [];
      L ->
        lists:flatten (
          [ [graphite_normalize_token (K), graphite_normalize_token (V)]
               || {K, V} <- L,
                  K =/= <<"stat">> ]
        )
    end,
  {HostDir, AggregateDir} = mondemand_backend_stats_rrd_filecache:get_dirs(),
  % mondemand raw data will go in the "md" directory, aggregates will
  % go in the "agg" directory
  InnerPath =
    case IsAggregate of
      false -> HostDir;
      true -> AggregateDir
    end,
  FilePath =
    filename:join(
      [ Prefix++"g",  % append a 'g' to prefix since this is graphite
        graphite_normalize_token (ProgId),
        InnerPath,
        graphite_normalize_token (MetricName)
      ]
      ++ ContextParts
      ++ [ "host", graphite_normalize_host (Host) ]),
  FileName = list_to_binary ([atom_to_list (MetricType),".rrd"]),
  {FilePath, FileName}.

maybe_create (Types, AggregatedType, File) ->

  {Type, SubType} =
     case Types of
       {T, S} -> {T, S};
       T -> {T, undefined}
     end,

  case file:read_file_info (File) of
    {ok, I} -> {ok, I};
    _ ->
      case Type of
        counter -> create_counter (File);
        gauge -> create_gauge (File);
        statset -> create_summary (SubType, AggregatedType, File);
        _ -> create_counter (File) % default is counter
      end
  end.

create_counter (File) ->
  % creates an RRD file of 438120 bytes
  erlrrd:create ([
      io_lib:fwrite ("~s",[File]),
      " --step \"60\""
      " --start \"now - 90 days\""
      " \"DS:value:DERIVE:900:0:U\""
      " \"RRA:AVERAGE:0.5:1:44640\""  % 31 days of 1 minute samples
      " \"RRA:AVERAGE:0.5:15:9600\""  % 100 days of 15 minute intervals
      " \"RRA:AVERAGE:0.5:1440:400\"" % 400 day of 1 day intervals
    ]).

create_gauge (File) ->
  % creates an RRD file of 438128 bytes
  erlrrd:create ([
      io_lib:fwrite ("~s",[File]),
      " --step \"60\""
      " --start \"now - 90 days\""
      " \"DS:value:GAUGE:900:U:U\""
      " \"RRA:AVERAGE:0.5:1:44640\""  % 31 days of 1 minute samples
      " \"RRA:AVERAGE:0.5:15:9600\""  % 100 days of 15 minute intervals
      " \"RRA:AVERAGE:0.5:1440:400\"" % 400 days of 1 day intervals
    ]).

create_summary (SubType, AggregatedType, File) ->
  RRDType =
    case AggregatedType of
      % statset's being used direct from client
      % will not have an aggregated type and are
      % reset each minute so are gauges
      undefined -> "GAUGE";
      % counter's will mostly be counters (in the RRD case we use DERIVE),
      % except for the count subtype which will be a gauge as it is always
      % for the last time period
      <<"counter">> ->
        case SubType of
          count -> "GAUGE";
          _ -> "DERIVE"
        end;
      <<"gauge">> -> "GAUGE";
      _ -> "GAUGE"
    end,
  erlrrd:create ([
      io_lib:fwrite ("~s",[File]),
      " --step \"60\""
      " --start \"now - 90 days\"",
      io_lib:fwrite (" \"DS:value:~s:900:U:U\"",[RRDType]),
      " \"RRA:AVERAGE:0.5:1:44640\""   % 31 days of 1 minute samples
      " \"RRA:AVERAGE:0.5:15:9600\""   % 100 days of 15 minute intervals
      " \"RRA:AVERAGE:0.5:1440:1200\"" % 1200 days of 1 day intervals
  ]).
