%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 12月 2020 上午10:48
%%%-------------------------------------------------------------------
-module(emqx_bridge_kafka_sup).
-author("root").
-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-vsn("4.2.2").

start_link() ->
  supervisor:start_link({local, emqx_bridge_kafka_sup}, emqx_bridge_kafka_sup, []).

init([]) -> {ok, {{one_for_one, 10, 100}, []}}.

