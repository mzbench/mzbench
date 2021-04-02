-module(mzb_spawn).

%% API
-export([
  spawn/1, spawn_link/1, spawn_monitor/1
]).

spawn(Fun) ->
  proc_lib:spawn(Fun).

spawn_link(Fun) ->
  proc_lib:spawn_link(Fun).

spawn_monitor(Fun) ->
  proc_lib:spawn_opt(erlang,apply,[Fun,[]],[monitor]).
