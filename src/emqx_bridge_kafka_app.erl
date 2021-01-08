%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 12月 2020 上午10:48
%%%-------------------------------------------------------------------
-module(emqx_bridge_kafka_app).
-author("root").
-include("../include/emqx_bridge_kafka.hrl").
-behaviour(application).

-emqx_plugin(?MODULE).
-export([start/2, stop/1, prep_stop/1]).

-vsn("4.2.2").

start(_Type, _Args) ->
  {ok, _} = application:ensure_all_started(wolff),
  Servers = application:get_env(emqx_bridge_kafka, servers, [{"localhost", 9092}]),
  ConnStrategy = application:get_env(emqx_bridge_kafka, connection_strategy, per_partition),
  RefreshInterval = application:get_env(emqx_bridge_kafka, min_metadata_refresh_interval, 5000),
  SockOpts = application:get_env(emqx_bridge_kafka, sock_opts, []),
  ClientCfg = #{extra_sock_opts => SockOpts, connection_strategy => ConnStrategy, min_metadata_refresh_interval => RefreshInterval},
  ClientCfg1 = case application:get_env(emqx_bridge_kafka, query_api_versions, true) of
                 true -> ClientCfg;
                 false -> ClientCfg#{query_api_versions => false}
               end,
  ClientId = <<"emqx_bridge_kafka">>,
  {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, Servers, ClientCfg1),
  {ok, Sup} = emqx_bridge_kafka_sup:start_link(),
  emqx_bridge_kafka:register_metrics(),
  case emqx_bridge_kafka:load(ClientId) of
    {ok, []} ->
      logger:error("Start emqx_bridge_kafka fail"),
      wolff:stop_and_delete_supervised_client(ClientId);
    {ok, NProducers} ->
      emqx_bridge_kafka_cli:load(),
      {ok, Sup, #{client_id => ClientId, n_producers => NProducers}}
  end.

prep_stop(State) ->
  emqx_bridge_kafka:unload(),
  emqx_bridge_kafka_cli:unload(),
  State.

stop(#{client_id := ClientId,
  n_producers := NProducers}) ->
  lists:foreach(fun (Producers) ->
    wolff:stop_and_delete_supervised_producers(Producers)
                end,
    NProducers),
  ok = wolff:stop_and_delete_supervised_client(ClientId).


