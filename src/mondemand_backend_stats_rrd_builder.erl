-module (mondemand_backend_stats_rrd_builder).

-behaviour (mondemand_backend_worker).

-include ("mondemand_backend_stats_rrd_internal.hrl").

%% API
-export ([ calculate_context/3,
           build/1
         ]).

%% mondemand_backend_worker callbacks
-export ([ create/1,
           connected/1,
           connect/1,
           send/2,
           destroy/1
]).

-record (state, { connection_config,
                  send_timeout,
                  recv_timeout,
                  connect_timeout,
                  connection
}).

-define (NAME, mdbes_rrd_builder).

%%====================================================================
%% API
%%====================================================================
calculate_context (Prefix, FileKey, Dirs) ->
  % normalize to binary to simplify rest of code
  ProgId = mondemand_util:binaryify (
             mondemand_backend_stats_rrd_key:prog_id (FileKey)
           ),
  MetricName = mondemand_util:binaryify (
                 mondemand_backend_stats_rrd_key:metric_name (FileKey)
               ),
  MetricType = mondemand_backend_stats_rrd_key:metric_type (FileKey),
  Host = mondemand_util:binaryify (
           mondemand_backend_stats_rrd_key:host (FileKey)
         ),
  Context = [ {mondemand_util:binaryify (K), mondemand_util:binaryify (V) }
              || {K, V}
              <- mondemand_backend_stats_rrd_key:context (FileKey)
            ],

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
  {LegacyFileDir, LegacyFileName} =
     legacy_rrd_path (FullyQualifiedPrefix, ProgId, MetricType,
                      MetricName, Host, Context),

  {GraphiteFileDir, GraphiteFileName} =
     graphite_rrd_path (FullyQualifiedPrefix, ProgId, MetricType,
                        MetricName, Host, Context,
                        AggregatedType =/= undefined,
                        Dirs),

  #mdbes_rrd_builder_context {
    file_key = FileKey,
    metric_type = MetricType,
    aggregated_type = AggregatedType,
    legacy_rrd_file_dir = LegacyFileDir,
    legacy_rrd_file_name = LegacyFileName,
    graphite_rrd_file_dir = GraphiteFileDir,
    graphite_rrd_file_name = GraphiteFileName
  }.

build (BuilderContext) ->
  mondemand_backend_worker_pool_sup:process (mdbes_rrd_builder_pool, { udp, 0, 0, 0, BuilderContext } ).

%%====================================================================
%% mondemand_backend_worker callbacks
%%====================================================================
create (Config) ->
  ConnectionConfig =
    case proplists:get_value (path, Config, undefined) of
      undefined ->
        Host = proplists:get_value (host, Config, "127.0.0.1"),
        Port = proplists:get_value (port, Config, 11211),
        {Host, Port};
      Path ->
        Path
    end,
  ConnectTimeout = proplists:get_value (connect_timeout, Config, 1000),
  SendTimeout = proplists:get_value (send_timeout, Config, 100),
  RecvTimeout = proplists:get_value (recv_timeout, Config, 50),

  {ok, #state { connection_config = ConnectionConfig,
                connect_timeout = ConnectTimeout,
                send_timeout = SendTimeout,
                recv_timeout = RecvTimeout,
                connection = undefined
              }}.

connected (#state { connection = undefined }) -> false;
connected (_) -> true.

connect (State = #state { connection_config = ConnectionConfig,
                          connect_timeout = ConnectTimeout,
                          send_timeout = SendTimeout,
                          recv_timeout = RecvTimeout
                        }) ->
  case rrdcached_client:open (ConnectionConfig, ConnectTimeout,
                              SendTimeout, RecvTimeout) of
    {ok, Client} ->
      {ok, State#state { connection = Client }};
    Error ->
      {Error, State}
  end.

% TODO: replace this hack with better way to pass CREATE context
% data to maybe_create_files.  Sample sys.config pool params:
%
%     { mondemand_backend_stats_rrd_builder,
%       [
%         { path, "/var/run/rrdcached/rrdcached.sock" },
%         { worker_mod, mondemand_backend_stats_rrd_builder },
%         { recv_timeout, infinity },
%         { send_timeout, 5000 },
%         { pass_raw_data, true }
%       ]
%     },
%
send (State = #state {connection = Client0}, {udp, _, _, _, 
      Context = #mdbes_rrd_builder_context { file_key = FileKey }}) ->
  case maybe_create_files (Client0, Context) of
    {ok, _} ->
      mondemand_backend_stats_rrd_filecache:mark_created (FileKey),
      {ok, State};
    E ->
      mondemand_backend_stats_rrd_filecache:mark_error (FileKey, E),
      {error, State}
  end.

destroy (#state {connection = Client}) ->
  rrdcached_client:close (Client).

%%====================================================================
%% internal functions
%%====================================================================
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

graphite_normalize_host (Host) ->
  case re:run(Host,"([^\.]+)", [{capture, all_but_first, binary}]) of
    {match, [H]} -> H;
    nomatch -> Host
  end.

graphite_normalize_token (Token) ->
  re:replace (Token, "\\W", "_", [global, {return, binary}]).


graphite_rrd_path (Prefix, ProgId,
                   {statset, SubType}, MetricName, Host, Context,
                   IsAggregate, Dirs) ->
  graphite_rrd_path (Prefix, ProgId, SubType, MetricName, Host, Context,
                     IsAggregate, Dirs);
graphite_rrd_path (Prefix, ProgId,  MetricType, MetricName, Host, Context,
                   IsAggregate, Dirs) ->
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
  {HostDir, AggregateDir} = Dirs,
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

maybe_create_files ( Client,
                     #mdbes_rrd_builder_context {
                       metric_type = MetricType,
                       aggregated_type = AggregatedType,
                       legacy_rrd_file_dir = LegacyFileDir,
                       legacy_rrd_file_name = LegacyFileName,
                       graphite_rrd_file_dir = GraphiteFileDir,
                       graphite_rrd_file_name = GraphiteFileName
                     } ) ->
  RRDFile = filename:join ([LegacyFileDir, LegacyFileName]),

  case maybe_create (Client, MetricType, AggregatedType, RRDFile) of
    {ok, _} ->
       {ok, RRDFile};
    {_, {status, 0, _}} ->
      case mondemand_server_util:mkdir_p (GraphiteFileDir) of
        ok ->
          GRRDFile = filename:join ([GraphiteFileDir, GraphiteFileName]),

          % making symlinks for the moment
          file:make_symlink (RRDFile, GRRDFile),
          {ok, RRDFile};
        E ->
          {error, {cant_create_dir, GraphiteFileDir, E}}
      end;
    {_, {status, _, Error}} ->
      error_logger:error_msg (
        "Unable to create '~p' because of ~p",[RRDFile, Error]),
      {error, Error};
    Unknown ->
      error_logger:error_msg (
        "Unable to create '~p' because of unknown ~p",[RRDFile, Unknown]),
      {error, Unknown}
  end.

maybe_create (Client, Types, AggregatedType, File) ->
  {Type, SubType} =
     case Types of
       {T, S} -> {T, S};
       T -> {T, undefined}
     end,

  case file:read_file_info (File) of
    {ok, I} -> {ok, I};
    _ ->
      case Type of
        counter -> create_counter (Client, File);
        gauge -> create_gauge (Client, File);
        statset -> create_summary (Client, SubType, AggregatedType, File);
        _ -> create_counter (Client, File) % default is counter
      end
  end.

create_rrd (Client, File, RRDType, DSMin, DaysMax) ->
  % creates an RRD file of 438120 bytes
  TS = mondemand_server_util:seconds_since_epoch () - 60,
  rrdcached_client:create ( Client, File, [
      io_lib:fwrite ("-b ~B",[TS]),
      " -s 60"
      " -O",
      io_lib:fwrite (" DS:value:~s:900:~s:U",[RRDType, DSMin]),
      " RRA:AVERAGE:0.5:1:44640"  % 31 days of 1 minute samples
      " RRA:AVERAGE:0.5:15:9600",  % 100 days of 15 minute intervals
      io_lib:fwrite (" RRA:AVERAGE:0.5:1440:~s", [DaysMax]) % DaysMax days of 1 day intervals
    ] ).

create_counter (Client, File) ->
  % creates an RRD file of 438120 bytes
  create_rrd (Client, File, "DERIVE", "0", "400").

create_gauge (Client, File) ->
  % creates an RRD file of 438128 bytes
  create_rrd (Client, File, "GAUGE", "U", "400").

create_summary (Client, SubType, AggregatedType, File) ->
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
  create_rrd (Client, File, RRDType, "U", "1200").

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


-endif.
