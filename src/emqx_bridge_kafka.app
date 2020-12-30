{application,emqx_bridge_kafka,
             [{description,"EMQ X Bridge to Kafka"},
              {vsn,"4.2.2"},
              {modules,[
                emqx_bridge_kafka,
                emqx_bridge_kafka_app,
                emqx_bridge_kafka_cli,
                emqx_bridge_kafka_sup]},
              {registered,[emqx_bridge_kafka_sup]},
              {applications,[kernel,stdlib,wolff,erlavro,jsx,brod]},
              {mod,{emqx_bridge_kafka_app,[]}},
              {relup_deps,[emqx]}]}.

%% wolff: kafka https://github.com/bgitter/wolff.git
%% erlavro: erlang type  https://github.com/klarna/erlavro.git
%% jsx:  an erlang application for consuming, producing and manipulating json  https://github.com/talentdeficit/jsx.git
%% brod: Apache Kafka client library for Erlang/Elixir https://github.com/klarna/brod.git