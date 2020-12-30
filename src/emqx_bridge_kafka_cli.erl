%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 12月 2020 上午10:48
%%%-------------------------------------------------------------------
-module(emqx_bridge_kafka_cli).
-author("root").

-export([load/0, kafka_stats/1, unload/0]).

-vsn("4.2.2").

load() ->
  emqx_ctl:register_command(kafka_stats, {emqx_bridge_kafka_cli, kafka_stats}, []).

kafka_stats([]) ->
  lists:foreach(fun ({Stat, Val}) ->
    io:format("~-20s: ~w~n", [Stat, Val])
                end,
    maps:to_list(wolff_stats:getstat()));
kafka_stats(_) ->
  [io:format("~-48s# ~s~n", [Cmd, Descr])
    || {Cmd, Descr}
    <- [{"kafka_stats", "Bridge kafka message stats"}]].

unload() -> emqx_ctl:unregister_command(kafka_stats).

