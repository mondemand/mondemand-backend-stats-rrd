-ifndef(_mondemand_backend_stats_rrd_internal_included).
-define(_mondemand_backend_stats_rrd_internal_included, yup).

% record which the builder can use to store some temporary info
-record (mdbes_rrd_builder_context, {
           file_key,
           metric_type,
           aggregated_type,
           legacy_rrd_file_dir,
           legacy_rrd_file_name,
           graphite_rrd_file_dir,
           graphite_rrd_file_name
         }).

-endif.
