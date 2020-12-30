%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 12月 2020 上午10:47
%%%-------------------------------------------------------------------
-module(emqx_bridge_kafka).
-author("root").
-include("../include/emqx_bridge_kafka.hrl").
-include("../include/emqx.hrl").


-export([register_metrics/0, load/1, unload/0]).

-export([wolff_callback/2]).

-export([on_client_connected/3,
  on_client_disconnected/4, on_session_subscribed/4,
  on_session_unsubscribed/4, on_message_publish/2,
  on_message_delivered/3, on_message_acked/3]).

-import(proplists, [get_value/2, get_value/3]).

-vsn("4.2.2").

register_metrics() ->
  [emqx_metrics:new(MetricName)
    || MetricName
    <- ['bridge.kafka.client_connected',
      'bridge.kafka.client_disconnected',
      'bridge.kafka.session_subscribed',
      'bridge.kafka.session_unsubscribed',
      'bridge.kafka.message_publish',
      'bridge.kafka.message_acked',
      'bridge.kafka.message_delivered']].

load(ClientId) ->
  HookList = parse_hook(application:get_env(emqx_bridge_kafka, hooks, [])),
  ReplayqDir0 = application:get_env(emqx_bridge_kafka, replayq_dir, false),
  ProducerCfg = application:get_env(emqx_bridge_kafka, producer, []),
  NProducers = lists:foldl(fun ({Hook, Filter, Key, Topic,
    Strategy, Seq, MessageFormat, PayloadFormat},
      Acc) ->
    SchemaEncoder = gen_encoder(Hook,
      MessageFormat),
    ReplayqDir = case ReplayqDir0 of
                   false -> ReplayqDir0;
                   _ ->
                     filename:join([ReplayqDir0,
                       node()])
                 end,
    Name =
      list_to_atom(lists:concat([atom_to_list(Hook),
        "_", Seq])),
    ProducerCfg1 = ProducerCfg ++
      [{partitioner, Strategy},
        {replayq_dir,
          ReplayqDir},
        {name, Name}],
    case
      wolff:ensure_supervised_producers(ClientId,
        Topic,
        maps:from_list(ProducerCfg1))
    of
      {ok, Producers} ->
        load_(Hook,
          {Filter, Producers, Key,
            SchemaEncoder,
            PayloadFormat}),
        [Producers | Acc];
      {error, Error} ->
        logger:error("Start topic:~p producers fail, error:~p",
          [Topic, Error]),
        wolff:stop_and_delete_supervised_producers(ClientId,
          Topic,
          Name),
        Acc
    end
                           end,
    [], HookList),
  io:format("~s is loaded.~n", [emqx_bridge_kafka]),
  {ok, NProducers}.

load_(Hook, Params) ->
  case Hook of
    'client.connected' ->
      emqx:hook(Hook,
        fun emqx_bridge_kafka:on_client_connected/3, [Params]);
    'client.disconnected' ->
      emqx:hook(Hook,
        fun emqx_bridge_kafka:on_client_disconnected/4,
        [Params]);
    'session.subscribed' ->
      emqx:hook(Hook,
        fun emqx_bridge_kafka:on_session_subscribed/4,
        [Params]);
    'session.unsubscribed' ->
      emqx:hook(Hook,
        fun emqx_bridge_kafka:on_session_unsubscribed/4,
        [Params]);
    'message.publish' ->
      emqx:hook(Hook,
        fun emqx_bridge_kafka:on_message_publish/2, [Params]);
    'message.acked' ->
      emqx:hook(Hook,
        fun emqx_bridge_kafka:on_message_acked/3, [Params]);
    'message.delivered' ->
      emqx:hook(Hook,
        fun emqx_bridge_kafka:on_message_delivered/3, [Params])
  end.

unload() ->
  HookList =
    parse_hook(application:get_env(emqx_bridge_kafka, hooks,
      [])),
  lists:foreach(fun ({Hook, _, _, _, _, _, _, _}) ->
    unload_(Hook)
                end,
    HookList),
  io:format("~s is unloaded.~n", [emqx_bridge_kafka]),
  ok.

unload_(Hook) ->
  case Hook of
    'client.connected' ->
      emqx:unhook(Hook,
        fun emqx_bridge_kafka:on_client_connected/3);
    'client.disconnected' ->
      emqx:unhook(Hook,
        fun emqx_bridge_kafka:on_client_disconnected/4);
    'session.subscribed' ->
      emqx:unhook(Hook,
        fun emqx_bridge_kafka:on_session_subscribed/4);
    'session.unsubscribed' ->
      emqx:unhook(Hook,
        fun emqx_bridge_kafka:on_session_unsubscribed/3);
    'message.publish' ->
      emqx:unhook(Hook,
        fun emqx_bridge_kafka:on_message_publish/2);
    'message.acked' ->
      emqx:unhook(Hook,
        fun emqx_bridge_kafka:on_message_acked/3);
    'message.delivered' ->
      emqx:unhook(Hook,
        fun emqx_bridge_kafka:on_message_delivered/3)
  end.

on_client_connected(ClientInfo, _ConnInfo,
    {_, Producers, Key, SchemaEncoder, _}) ->
  emqx_metrics:inc('bridge.kafka.client_connected'),
  ClientId = maps:get(clientid, ClientInfo, undefined),
  Username = maps:get(username, ClientInfo, undefined),
  Data = [{clientid, ClientId}, {username, Username},
    {node, a2b(node())},
    {ts, erlang:system_time(millisecond)}],
  msg_to_kafka(Producers,
    {feed_key(Key, {ClientId, Username}),
      data_format(Data, SchemaEncoder)}),
  ok.

on_client_disconnected(ClientInfo, {shutdown, Reason},
    ConnInfo, Rule)
  when is_atom(Reason); is_integer(Reason) ->
  on_client_disconnected(ClientInfo, Reason, ConnInfo,
    Rule);
on_client_disconnected(ClientInfo, Reason, _ConnInfo,
    {_, Producers, Key, SchemaEncoder, _})
  when is_atom(Reason); is_integer(Reason) ->
  emqx_metrics:inc('bridge.kafka.client_disconnected'),
  ClientId = maps:get(clientid, ClientInfo, undefined),
  Username = maps:get(username, ClientInfo, undefined),
  Data = [{clientid, ClientId}, {username, Username},
    {node, a2b(node())}, {reason, a2b(Reason)},
    {ts, erlang:system_time(millisecond)}],
  msg_to_kafka(Producers,
    {feed_key(Key, {ClientId, Username}),
      data_format(Data, SchemaEncoder)}),
  ok;
on_client_disconnected(_ClientInfo, Reason, _ConnInfo,
    _Envs) ->
  logger:error("Client disconnected reason:~p not encode "
  "json",
    [Reason]),
  ok.

on_session_subscribed(ClientInfo, Topic, Opts,
    {Filter, Producers, Key, SchemaEncoder, _}) ->
  case emqx_topic:match(Topic, Filter) of
    true ->
      ClientId = maps:get(clientid, ClientInfo, undefined),
      Username = maps:get(username, ClientInfo, undefined),
      emqx_metrics:inc('bridge.kafka.session_subscribed'),
      Data = format_sub_json(ClientId, Topic, Opts),
      msg_to_kafka(Producers,
        {feed_key(Key, {ClientId, Username, Topic}),
          data_format(Data, SchemaEncoder)});
    false -> ok
  end,
  ok.

on_session_unsubscribed(ClientInfo, Topic, Opts,
    {Filter, Producers, Key, SchemaEncoder, _}) ->
  case emqx_topic:match(Topic, Filter) of
    true ->
      ClientId = maps:get(clientid, ClientInfo, undefined),
      Username = maps:get(username, ClientInfo, undefined),
      emqx_metrics:inc('bridge.kafka.session_unsubscribed'),
      Data = format_sub_json(ClientId, Topic, Opts),
      msg_to_kafka(Producers,
        {feed_key(Key, {ClientId, Username, Topic}),
          data_format(Data, SchemaEncoder)});
    false -> ok
  end,
  ok.

on_message_publish(Msg = #message{topic = Topic,
  from = From, headers = Headers},
    {Filter, Producers, Key, SchemaEncoder,
      PayloadFormat}) ->
  case emqx_topic:match(Topic, Filter) of
    true ->
      emqx_metrics:inc('bridge.kafka.message_publish'),
      Data = format_pub_msg(Msg, PayloadFormat),
      Username = maps:get(username, Headers, <<>>),
      msg_to_kafka(Producers,
        {feed_key(Key, {From, Username, Topic}),
          data_format(Data, SchemaEncoder)});
    false -> ok
  end,
  {ok, Msg}.

on_message_acked(ClientInfo,
    Msg = #message{topic = Topic},
    {Filter, Producers, Key, SchemaEncoder,
      PayloadFormat}) ->
  case emqx_topic:match(Topic, Filter) of
    true ->
      emqx_metrics:inc('bridge.kafka.message_acked'),
      ClientId = maps:get(clientid, ClientInfo, undefined),
      Username = maps:get(username, ClientInfo, undefined),
      Data = format_revc_msg(ClientId, Username, Msg,
        PayloadFormat),
      msg_to_kafka(Producers,
        {feed_key(Key, {ClientId, Username, Topic}),
          data_format(Data, SchemaEncoder)});
    false -> ok
  end,
  {ok, Msg}.

on_message_delivered(ClientInfo,
    Msg = #message{topic = Topic},
    {Filter, Producers, Key, SchemaEncoder,
      PayloadFormat}) ->
  case emqx_topic:match(Topic, Filter) of
    true ->
      emqx_metrics:inc('bridge.kafka.message_delivered'),
      ClientId = maps:get(clientid, ClientInfo, undefined),
      Username = maps:get(username, ClientInfo, undefined),
      Data = format_revc_msg(ClientId, Username, Msg,
        PayloadFormat),
      msg_to_kafka(Producers,
        {feed_key(Key, {ClientId, Username, Topic}),
          data_format(Data, SchemaEncoder)});
    false -> ok
  end,
  {ok, Msg}.

parse_hook(Hooks) -> parse_hook(Hooks, [], 0).

parse_hook([], Acc, _Seq) -> Acc;
parse_hook([{Hook, Item} | Hooks], Acc, Seq) ->
  Params = emqx_json:decode(Item),
  Topic = get_value(<<"topic">>, Params),
  Filter = get_value(<<"filter">>, Params),
  Key = get_value(<<"key">>, Params),
  Strategy = b2a(get_value(<<"strategy">>, Params,
    <<"random">>)),
  NewSeq = Seq + 1,
  MessageFormat = get_value(<<"format">>, Params,
    <<"json">>),
  PayloadFormat =
    a2b(application:get_env(emqx_bridge_kafka,
      encode_payload_type, base64)),
  parse_hook(Hooks,
    [{l2a(Hook), Filter, Key, Topic, Strategy, NewSeq,
      MessageFormat, PayloadFormat}
      | Acc],
    NewSeq).

msg_to_kafka(Producers, {Key, JsonMsg}) ->
  try produce(Producers, Key, JsonMsg) catch
    Error:Reason:Stask ->
      logger:error("Call produce error: ~p, ~p",
        [Error, {Reason, Stask}])
  end.

produce(Producers, Key, JsonMsg)
  when is_list(JsonMsg) ->
  produce(Producers, Key, iolist_to_binary(JsonMsg));
produce(Producers, Key, JsonMsg) ->
  logger:debug("Produce key:~p, payload:~p",
    [Key, JsonMsg]),
  case application:get_env(emqx_bridge_kafka, produce,
    sync)
  of
    sync ->
      Timeout = application:get_env(emqx_bridge_kafka,
        produce_sync_timeout, 3000),
      wolff:send_sync(Producers,
        [#{key => Key, value => JsonMsg}], Timeout);
    async ->
      wolff:send(Producers, [#{key => Key, value => JsonMsg}],
        fun emqx_bridge_kafka:wolff_callback/2)
  end.

wolff_callback(_Partition, _BaseOffset) -> ok.

format_sub_json(ClientId, Topic, Opts) ->
  Qos = maps:get(qos, Opts, 0),
  [{clientid, ClientId}, {topic, Topic}, {qos, Qos},
    {node, a2b(node())},
    {ts, erlang:system_time(millisecond)}].

payload_format(Payload, PayloadFormat) ->
  case PayloadFormat of
    <<"base64">> -> base64:encode(Payload);
    _ -> Payload
  end.

format_pub_msg(Msg, PayloadFormat) ->
  #message{from = From, topic = Topic, payload = Payload,
    headers = Headers, qos = Qos, timestamp = Ts} =
    Msg,
  Username = maps:get(username, Headers, <<>>),
  [{clientid, From}, {username, Username}, {topic, Topic},
    {payload, payload_format(Payload, PayloadFormat)},
    {qos, Qos}, {node, a2b(node())}, {ts, Ts}].

format_revc_msg(ClientId, Username, Msg,
    PayloadFormat) ->
  #message{from = From, topic = Topic, payload = Payload,
    qos = Qos, timestamp = Ts} =
    Msg,
  [{clientid, ClientId}, {username, Username},
    {from, From}, {topic, Topic},
    {payload, payload_format(Payload, PayloadFormat)},
    {qos, Qos}, {node, a2b(node())}, {ts, Ts}].

data_format(Data, undefined) -> emqx_json:encode(Data);
data_format(Data, SchemaEncoder) ->
  AvroMsg = erlang:iolist_to_binary(SchemaEncoder(Data)),
  binary_header(5, AvroMsg).

binary_header(0, Binary) -> Binary;
binary_header(Cnt, Binary) ->
  binary_header(Cnt - 1, <<0:8, Binary/binary>>).

l2a(L) -> erlang:list_to_atom(L).

a2b(A) when is_atom(A) ->
  erlang:atom_to_binary(A, utf8);
a2b(A) -> A.

b2a(A) -> erlang:binary_to_atom(A, utf8).

feed_key(undefined, _) -> <<>>;
feed_key(<<"${clientid}">>, {ClientId, _Username}) ->
  ClientId;
feed_key(<<"${username}">>, {_ClientId, Username}) ->
  Username;
feed_key(<<"${clientid}">>,
    {ClientId, _Username, _Topic}) ->
  ClientId;
feed_key(<<"${username}">>,
    {_ClientId, Username, _Topic}) ->
  Username;
feed_key(<<"${topic}">>,
    {_ClientId, _Username, Topic}) ->
  Topic;
feed_key(Key, {_ClientId, _Username, Topic}) ->
  case re:run(Key, <<"{([^}]+)}">>,
    [{capture, all, binary}, global])
  of
    nomatch -> <<>>;
    {match, Match} ->
      TopicWords = emqx_topic:words(Topic),
      lists:foldl(fun ([_, Index], Acc) ->
        Word = lists:nth(binary_to_integer(Index),
          TopicWords),
        <<Acc/binary, Word/binary>>
                  end,
        <<>>, Match)
  end.

gen_encoder(_, <<"json">>) -> undefined;
gen_encoder('client.connected', _Format) ->
  {ok, SchemaJSON} =
    file:read_file(code:priv_dir(emqx_bridge_kafka) ++
    "/connect.avsc"),
  avro:make_simple_encoder(jsx:format(SchemaJSON), []);
gen_encoder('client.disconnected', _Format) ->
  {ok, SchemaJSON} =
    file:read_file(code:priv_dir(emqx_bridge_kafka) ++
    "/disconnect.avsc"),
  avro:make_simple_encoder(jsx:format(SchemaJSON), []);
gen_encoder('session.subscribed', _Format) ->
  {ok, SchemaJSON} =
    file:read_file(code:priv_dir(emqx_bridge_kafka) ++
    "/subscribe.avsc"),
  avro:make_simple_encoder(jsx:format(SchemaJSON), []);
gen_encoder('session.unsubscribed', _Format) ->
  {ok, SchemaJSON} =
    file:read_file(code:priv_dir(emqx_bridge_kafka) ++
    "/subscribe.avsc"),
  avro:make_simple_encoder(jsx:format(SchemaJSON), []);
gen_encoder('message.publish', _Format) ->
  {ok, SchemaJSON} =
    file:read_file(code:priv_dir(emqx_bridge_kafka) ++
    "/publish.avsc"),
  avro:make_simple_encoder(jsx:format(SchemaJSON), []);
gen_encoder('message.acked', _Format) ->
  {ok, SchemaJSON} =
    file:read_file(code:priv_dir(emqx_bridge_kafka) ++
    "/receive.avsc"),
  avro:make_simple_encoder(jsx:format(SchemaJSON), []);
gen_encoder('message.delivered', _Format) ->
  {ok, SchemaJSON} =
    file:read_file(code:priv_dir(emqx_bridge_kafka) ++
    "/receive.avsc"),
  avro:make_simple_encoder(jsx:format(SchemaJSON), []).

