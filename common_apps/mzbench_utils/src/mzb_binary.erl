-module(mzb_binary).

%% API
-export([
  format/1,
  join/2,
  merge/1,
  indent/2, indent/3,
  length/1,
  trim_right/2
]).

-compile({no_autoimport, [ length/1 ]}).

format(Format, Args) ->
  unicode:characters_to_binary(format(Format, Args)).

format(Formats) ->
  iolist_to_binary(lists:map(fun({Format, Args}) -> format(Format, Args) end, Formats)).

join([], _Sep) ->
  [];
join([H|T], Sep) ->
  iolist_to_binary([ H, [[Sep, X ] || X <- T]]).

merge(List) ->
  iolist_to_binary(List).

indent(<<>>, N, Default) -> indent(Default, N);
indent(Str, N, _) -> indent(Str, N).

indent(Str, N) ->
  Spaces = iolist_to_binary(lists:duplicate(N, <<" ">>)),
  join([[ Spaces, Line ] || Line <- binary:split(Str, <<"\n">>, [ global ])], <<"\n">>).

trim_right(<<>>, _) -> <<>>;
trim_right(Bin, Byte) ->
  case binary:last(Bin) =:= Byte of
    true ->
      trim_right(binary:part(Bin, { 0, byte_size(Bin) - 1 }), Byte);
    _ -> Bin
  end.

length(Bin) ->
  byte_size(Bin).