-module(rrdcached_client).

-export([flush/1,
         flushall/0,
         pending/1,
         forget/1,
         stats/0,
         update/2,
         command_set_extra/2,
         command_get_extra/1,
         command_get_file/1]).

-export([flush/2,
         flushall/1,
         pending/2,
         forget/2,
         stats/1,
         update/3,
         batch_start/1,
         batch_end/1
        ]).

-export([open/4,
         send/2,
         recv/1,
         close/1,
         send_command/2,
         record_to_string/1
        ]).

-record (client, {socket,
                  recv_timeout,
                  in_batch = false,
                  batch_lines = [],
                  type
                 }).

-record (command, {action,
                   file,
                   values,
                   extra
                  }).

record_to_string ([]) -> [];
record_to_string (L) when is_list (L) ->
  [ record_to_string (E) || E <- L ];
record_to_string (#command { action = flush,
                             file = File }) when File =/= undefined ->
  ["FLUSH ", File, "\n"];
record_to_string (#command { action = flushall }) ->
  "FLUSHALL\n";
record_to_string (#command { action = pending,
                             file = File }) when File =/= undefined ->
  ["PENDING", File, "\n"];
record_to_string (#command { action = forget,
                             file = File }) when File =/= undefined ->
  ["FORGET ", File, "\n"];
record_to_string (#command { action = stats }) ->
  "STATS\n";
record_to_string (#command { action = update,
                             file = File,
                             values = List })
  when File =/= undefined, is_list (List) ->
  ["UPDATE ",File," ",List,"\n"];
record_to_string (#command { action = batch_start }) ->
  "BATCH\n";
record_to_string (#command { action = batch_end }) ->
  ".\n".

flush (Filename) -> #command { action = flush, file = Filename }.
flushall () -> #command { action = flushall }.
pending (Filename) -> #command { action = pending, file = Filename }.
forget (Filename) -> #command { action = forget, file = Filename }.
stats () -> #command { action = stats }.
update (Filename, Value) -> #command { action = update, file = Filename, values = Value }.
batch_start () -> #command {action = batch_start}.
batch_end () -> #command {action = batch_end}.


flush (Client, Filename) ->
  send_command (Client, flush (Filename)).
flushall (Client) ->
  send_command (Client, flushall ()).
pending (Client, Filename) ->
  send_command (Client, pending (Filename)).
forget (Client, Filename) ->
  send_command (Client, forget (Filename)).
stats (Client) ->
  case send_command (Client, stats ()) of
    {NewClient, {ok, StatsResults}} ->
      {NewClient, {ok, stats_results_to_proplist (StatsResults)}};
    Other ->
      Other
  end.

stats_results_to_proplist (StatsResults) ->
  lists:foldl (fun ({status,_,_}, A) -> A;
                   ({line,_,Stat}, A) ->
                      % stats come back as "Key: Value\n"
                      [Key, ValueStr] = string:tokens (Stat, ": \n"),
                      Value = case string:to_integer (ValueStr) of
                                {error, _} -> -1;
                                {I, _} -> I
                              end,
                      [ {Key, Value} | A ]
               end,
               [],
               StatsResults).

update (Client, Filename, Value) ->
  send_command (Client, update (Filename, Value)).
batch_start (Client) ->
  case send_command (Client, batch_start()) of
    {NewClient, {status,0,"Go ahead.  End with dot '.' on its own line.\n"}} ->
      { NewClient#client {in_batch = true}, ok };
    Other -> Other
  end.
batch_end (Client = #client {in_batch = true}) ->
  % first turn off batch mode, so that we wait for a response
  NewClient = Client#client {in_batch = false },
  case send_command (NewClient, batch_end()) of
    {FinalClient = #client { batch_lines = BatchLines }, {ok, Result} } ->
      case Result of
        [{status, 0, _}] ->
          % no errors from batch
          {FinalClient#client { batch_lines = []}, ok};
        [{status, _, "errors\n"} | R ] ->
          % had errors, so figure out which files they were
          Input = list_to_tuple (lists:reverse(lists:flatten (BatchLines))),
          Res =
            lists:map (fun ({line, _, Msg}) ->
                         case parse_error_line (Msg) of
                           {line, I, Err} -> { element (I, Input), Err };
                           E -> {unkwown, E}
                         end
                       end,
                       R),
          {FinalClient#client {batch_lines = []}, {error, Res}}
      end;
    {ErrorClient, Other}-> {ErrorClient#client { batch_lines = []}, Other}
  end.

command_set_extra (Command = #command {}, Extra) ->
  Command#command { extra = Extra }.
command_get_extra (#command { extra = Extra }) -> Extra.

command_get_file (#command { file = File }) -> File.

send_command (Client = #client { in_batch = true, batch_lines = BatchLines },
              Command) ->
  case send (Client, Command) of
    ok -> {Client#client {batch_lines =
                          [lists:reverse (lists:flatten (Command)) | BatchLines ]}, ok};
    SendError -> {Client, {error, send, SendError}}
  end;
send_command (Client, Command) ->
  case send (Client, Command) of
    ok ->
      case recv (Client) of
        {ok, Line} ->
          case process_line (Line) of
            {expecting, N, M} ->
              recv_until_done (Client, N, N, [{status, N, M}]);
            E -> {Client, E}
          end;
        ReceiveError -> {Client, {error, recv, ReceiveError}}
      end;
    SendError -> {Client, {error, send, SendError}}
  end.

recv_until_done (Client, 0, _, A) ->
  {Client, {ok, lists:reverse (A)}};
recv_until_done (Client, N, Expected, A) ->
  case recv (Client) of
    {ok, Line} ->
      recv_until_done (Client, N - 1, Expected,
                       [{line, Expected - N + 1, Line} | A]);
    ReceiveError -> {Client, {error, recv, ReceiveError, lists:reverse (A)}}
  end.


open ({Host, Port}, ConnectTimeout, SendTimeout, RecvTimeout) ->
  case
    gen_tcp:connect (Host, Port,
                     [{mode, list}, {packet, line},
                      {active, false}, {keepalive, true},
                      {send_timeout, SendTimeout}], ConnectTimeout)
  of
    {ok, Socket} ->
      {ok, #client {socket = Socket,
                    recv_timeout = RecvTimeout,
                    in_batch = false,
                    type = tcp
                   }
      };
    E -> E
  end;
open (File, ConnectTimeout, SendTimeout, RecvTimeout) ->
  case
    afunix:connect (File,
                    [{mode, list}, {packet, line},
                     {active, false}, {keepalive, true},
                     {send_timeout, SendTimeout}], ConnectTimeout)
  of
    {ok, Socket} ->
      {ok, #client {socket = Socket,
                    recv_timeout = RecvTimeout,
                    in_batch = false,
                    type = afunix
                   }
      };
    E -> E
  end.

send (#client {socket = Socket, type = tcp}, Command) ->
  gen_tcp:send (Socket, record_to_string (Command));
send (#client {socket = Socket, type = afunix}, Command) ->
  afunix:send (Socket, record_to_string (Command)).

recv (#client {socket = Socket, recv_timeout = RecvTimeout, type = tcp}) ->
  gen_tcp:recv (Socket, 0, RecvTimeout);
recv (#client {socket = Socket, recv_timeout = RecvTimeout, type = afunix}) ->
  afunix:recv (Socket, 0, RecvTimeout).

close (undefined) -> ok;
close (#client {socket = Socket, type = tcp}) ->
  gen_tcp:close (Socket);
close (#client {socket = Socket, type = afunix}) ->
  afunix:close (Socket).

% From RRDCACHED manpage
% The daemon answers with a line consisting of a status code and a short
% status message, separated by one or more space characters. A negative
% status code signals an error, a positive status code or zero signal
% success. If the status code is greater than zero, it indicates the
% number of lines that follow the status line.
%
% Examples:
%
%   0 Success<LF>
%
%   2 Two lines follow<LF>
%   This is the first line<LF>
%   And this is the second line<LF>
%
% Also from RRDCACHED manpage
%
% Command processing is finished when the client sends a dot (".") on
% its own line.  After the client has finished, the server responds
% with an error count and the list of error messages (if any).  Each
% error messages indicates the number of the command to which it
% corresponds, and the error message itself.  The first user command
% after BATCH is command number one.
%
% client:  BATCH
% server:  0 Go ahead.  End with dot '.' on its own line.
% client:  UPDATE x.rrd 1223661439:1:2:3            <--- command #1
% client:  UPDATE y.rrd 1223661440:3:4:5            <--- command #2
% client:  and so on...
% client:  .
% server:  2 Errors
% server:  1 message for command 1
% server:  12 message for command 12

process_line (Line) ->
  case parse_status_line (Line) of
    {status, N, StatusMessage} when N < 0 ->
      parse_status_message (StatusMessage);
    {status, N, M} when N > 0 ->
      {expecting, N, M};
    Other ->
      Other
  end.

parse_status_line (Status) ->
  case string:to_integer (Status) of
    {error, E} -> {error, {parse_status_line, E}};
    % string:to_integer/1 returns {integer, Rest}, we then strip
    % a SPACE (decimal value of 32), and check to see if there were errors
    {N, [32|R]} -> {status, N, R}
  end.

parse_error_line (ErrorLine) ->
  case string:to_integer (ErrorLine) of
    {error, E} -> {error, {parse_error_line, E}};
    {N, [32|R]} -> {line, N, parse_status_message (R)}
  end.

parse_status_message ("No such file or directory\n") ->
  {error, missing_file};
parse_status_message ("No such file: " ++ _File) ->
  {error, no_file};
parse_status_message ("stat failed with error 111.\n") ->
  % this is a very strange file, but I think should just result in removing
  % from the cache
  {error, no_file};
parse_status_message ("stat failed with error 36.\n") ->
  {error, filenametoolong};
parse_status_message ("illegal attempt to update using time " ++ _Time) ->
  {error, timestamp};
parse_status_message (Line) ->
  {error, {unknown, Line}}.

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


-endif.
