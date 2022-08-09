%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugin_kafka).

%% for #message{} record
%% no need for this include if we call emqx_message:to_map/1 to convert it to a map
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

%% for logging
-include_lib("emqx/include/logger.hrl").

-export([ load/1
        , unload/0
        ]).

%% Client Lifecircle Hooks
-export([ on_client_connected/3
        , on_client_disconnected/4
        ]).

%% Message Pubsub Hooks
-export([ on_message_publish/2
        ]).

kafka_init(_Env) ->
    ?SLOG(info, "Start to init emqx plugin kafka...... ~n"),
    % {ok, AddressList}=application:get_env(emqx_plugin_kafka, address_list),
    % ?SLOG(info, "[KAFKA PLUGIN]KafkaAddressList = ~p~n", [AddressList]),
    % {ok, KafkaConfig} = application:get_env(emqx_plugin_kafka, kafka_config),
    % ?SLOG(info,"[KAFKA PLUGIN]KafkaConfig = ~p~n", [KafkaConfig]),
    % {ok, KafkaTopic} = application:get_env(emqx_plugin_kafka, topic),
    % ?SLOG(info, "[KAFKA PLUGIN]KafkaTopic = ~s~n", [KafkaTopic]),
    {ok, _} = application:ensure_all_started(brod),
    ok = brod:start_client([{"172.17.16.58", 9092}], client),
    ok = brod:start_producer(client, <<"test">>, []),
    ?SLOG(info, "Init emqx plugin kafka successfully.....~n"),
    {M, S, _} = os:timestamp(),
    Ts = M*1000000+S,
    ok = brod:produce_sync(client, <<"test">>, 0, <<"key2">>, integer_to_list(Ts)),
    ok.

%% Called when the plugin application start
load(Env) ->
    kafka_init([Env]),
    emqx_hooks:add('client.connected',    {?MODULE, on_client_connected, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('client.disconnected', {?MODULE, on_client_disconnected, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('message.publish',     {?MODULE, on_message_publish, [Env]}, ?HP_HIGHEST).

%%--------------------------------------------------------------------
%% Client Lifecircle Hooks
%%--------------------------------------------------------------------

% on_client_connect(ConnInfo, Props, _Env) ->
    %% this is to demo the usage of EMQX's structured-logging macro
    %% * Recommended to always have a `msg` field,
    %% * Use underscore instead of space to help log indexers,
    %% * Try to use static fields
    % ?SLOG(debug, #{msg => "demo_log_msg_on_client_connect",
                %    conninfo => ConnInfo,
                %    props => Props}),
    % {ok, Props}.

on_client_connected(ClientInfo = #{clientid := ClientId}, ConnInfo, _Env) ->
    format_connected(ClientInfo, ConnInfo, ClientId),
    ?SLOG(debug, #{msg=>"on client connected", clientInfo=>ClientInfo, connInfo => ConnInfo, clientId=> ClientId}).

on_client_disconnected(ClientInfo = #{clientid := ClientId}, ReasonCode, ConnInfo, _Env) ->
    format_disconnected(ClientInfo, ConnInfo, ClientId, ReasonCode),
    ?SLOG(debug, #{msg=>"on client disconnected", clientInfo=>ClientInfo, connInfo => ConnInfo, reasonCode=> ReasonCode,clientId=> ClientId}).


%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->

    Timestamp = Message#message.timestamp,
    Payload = Message#message.payload,
    Username = emqx_message:get_header(username, Message),
    Topic = Message#message.topic,
    MsgType = <<"publish">>,
    From = Message#message.from,
    Qos = Message#message.qos,
    Retain = emqx_message:get_flag(retain, Message),

    % compress json string
    Payload1 = json_minify(Payload),
    Base64Payload =  base64:encode_to_string(Payload1),

    MsgBody = [
        {ts, Timestamp},
        {payload, iolist_to_binary(Base64Payload)},
        {device_id, Username},
        {topic, Topic},
        {action, MsgType},
        {client_id, From},
        {qos, Qos},
        {retain, Retain}
        ],
    send_kafka(MsgBody, Username),


    {ok, Message}.

%% Called when the plugin application stop
unload() ->
    emqx_hooks:del('client.connected',    {?MODULE, on_client_connected}),
    emqx_hooks:del('client.disconnected', {?MODULE, on_client_disconnected}),
    emqx_hooks:del('message.publish',     {?MODULE, on_message_publish}).


send_kafka(MsgBody, Username) -> 
    {ok, Mb} = emqx_json:safe_encode(MsgBody),
    Pl = iolist_to_binary(Mb),
    brod:produce_cb(client, <<"test">>, hash, Username, Pl, fun(_,_) -> ok end),
    ok.

format_connected(ClientInfo, ConnInfo, ClientId)->
    Ts = maps:get(connected_at, ConnInfo),
    Username = maps:get(username, ClientInfo),
    Action = <<"connected">>,
    Keepalive = maps:get(keepalive, ConnInfo),
    {IpAddr, _Port} = maps:get(peername, ConnInfo),
    Online = 1,
    Payload = [
        {action, Action},
        {device_id, Username},
        {keepalive, Keepalive},
        {ipaddress, iolist_to_binary(ntoa(IpAddr))},
        {ts, Ts},
        {client_id, ClientId},
        {online, Online}
    ],
    send_kafka(Payload, Username).

format_disconnected(ClientInfo, ConnInfo, ClientId, ReasonCode)->
    Ts = maps:get(connected_at, ConnInfo),
    Username = maps:get(username, ClientInfo),
    Action = <<"disconnected">>,
    Online = 0,
    Payload = [
        {action, Action},
        {device_id, Username},
        {client_id, ClientId},
        {reason, ReasonCode},
        {ts, Ts},
        {online, Online}
    ],

    send_kafka(Payload, Username),
    ok.

ntoa({0, 0, 0, 0, 0, 16#ffff, AB, CD}) ->
  inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
  inet_parse:ntoa(IP).

json_minify(Payload)->
    IsJson = jsx:is_json(Payload),
    if 
         IsJson ->
            jsx:minify(Payload);
        true ->
            Payload
    end.
