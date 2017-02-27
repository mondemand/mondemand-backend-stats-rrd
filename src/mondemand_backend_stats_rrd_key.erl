-module(mondemand_backend_stats_rrd_key).

-export([new/5,
         prog_id/1,
         metric_type/1,
         metric_name/1,
         host/1,
         context/1
        ]).

new (ProgId, MetricType, MetricName, Host, Context) ->
  {ProgId, MetricType, MetricName, Host, lists:sort(Context)}.

prog_id({P,_,_,_,_}) -> P.
metric_type({_,T,_,_,_}) -> T.
metric_name({_,_,N,_,_}) -> N.
host({_,_,_,H,_}) -> H.
context({_,_,_,_,C}) -> C.

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

key_test_ () ->
  [ fun() ->
      K = new (prog_id,gauge,name,host,[]),
      ?assertEqual (prog_id, prog_id (K)),
      ?assertEqual (gauge, metric_type (K)),
      ?assertEqual (name, metric_name (K)),
      ?assertEqual (host, host (K)),
      ?assertEqual ([], context (K))
    end,
    fun() ->
      K = new (prog_id,gauge,name,host,[{c,d},{a,b}]),
      ?assertEqual (prog_id, prog_id (K)),
      ?assertEqual (gauge, metric_type (K)),
      ?assertEqual (name, metric_name (K)),
      ?assertEqual (host, host (K)),
      ?assertEqual ([{a,b},{c,d}], context (K))
    end
  ].

-endif.
