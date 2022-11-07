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
    ?SLOG(warning, "Start to init emqx plugin kafka...... ~n"),
    {ok, _} = application:ensure_all_started(brod),
    ok = brod:start_client([{"kafka-cluster", 9092}], client),
    brod:start_producer(client, <<"mqttThingProperty">>, []),
    brod:start_producer(client, <<"mqttThingEvent">>, []),
    brod:start_producer(client, <<"mqttThingService">>, []),
    brod:start_producer(client, <<"mqttSystemOta">>, []),
    brod:start_producer(client, <<"mqttSystemShadow">>, []),
    brod:start_producer(client, <<"mqttdisconn">>, []),
    brod:start_producer(client, <<"mqttMessagePush">>, []),
    ?SLOG(info, "Init emqx plugin kafka successfully.....~n"),
    % {M, S, _} = os:timestamp(),
    % Ts = M*1000000+S,
    % ok = brod:produce_sync(client, <<"test">>, 0, <<"key2">>, integer_to_list(Ts)),
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
    MsgId = Message#message.id,

    % compress json string
    Payload1 = json_minify(Payload),

    MsgBody = [
        {msg_id, binary:encode_hex(MsgId)},
        {ts, Timestamp},
        {payload, Payload1},
        {device_id, Username},
        {topic, Topic},
        {action, MsgType},
        {client_id, From},
        {qos, Qos},
        {retain, Retain}
        ],
    
    TopicStr = binary_to_list(Topic),
    TopicProperty = string:str(TopicStr, "$thing/req/property"),
    TopicEvent = string:str(TopicStr, "$thing/req/event"),
    TopicService = string:str(TopicStr, "$thing/up/service"),
    TopicOta = string:str(TopicStr, "$ota/req"),
    TopicShadow = string:str(TopicStr, "$shadow/update"),
    TopicPush = string:str(TopicStr, "$push/up"),

    if 
        TopicProperty > 0 ->
            send_kafka(MsgBody, Username, <<"mqttThingProperty">>);
        TopicEvent > 0 ->
            send_kafka(MsgBody, Username, <<"mqttThingEvent">>);
        TopicService > 0 ->
            send_kafka(MsgBody, Username, <<"mqttThingService">>);
        TopicOta > 0 ->
            send_kafka(MsgBody, Username,<<"mqttSystemOta">>);
        TopicOta > 0 ->
            send_kafka(MsgBody, Username,<<"mqttSystemOta">>);
        TopicShadow > 0 ->
            send_kafka(MsgBody, Username, <<"mqttSystemShadow">>);
        TopicPush > 0 ->
            send_kafka(MsgBody, Username, <<"mqttMessagePush">>);
        true -> true
    end,
    
    
    {ok, Message}.

%% Called when the plugin application stop
unload() ->
    emqx_hooks:del('client.connected',    {?MODULE, on_client_connected}),
    emqx_hooks:del('client.disconnected', {?MODULE, on_client_disconnected}),
    emqx_hooks:del('message.publish',     {?MODULE, on_message_publish}).


send_kafka(MsgBody, Username, KafkaTopic) -> 
    {ok, Mb} = emqx_json:safe_encode(MsgBody),
    Pl = iolist_to_binary(Mb),
    brod:produce_cb(client, KafkaTopic, hash, Username, Pl, fun(_,_) -> ok end),
    % Res = brod:produce_sync(client, KafkaTopic, hash, Username, Pl),
    % ?SLOG(debug, #{msg=>"send kafka ok",res=>Res}),
    ok.

format_connected(ClientInfo, ConnInfo, ClientId)->
    Ts = maps:get(connected_at, ConnInfo),
    Username = maps:get(username, ClientInfo),
    Action = <<"connected">>,
    Keepalive = maps:get(keepalive, ConnInfo),
    {IpAddr, _Port} = maps:get(peername, ConnInfo),
    IsSuperuser = maps:get(is_superuser, ClientInfo),
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
    if
        not IsSuperuser ->
            send_kafka(Payload, Username, <<"mqttdisconn">>);
        true -> ok
    end.

format_disconnected(ClientInfo, ConnInfo, ClientId, ReasonCode)->
    Ts = maps:get(connected_at, ConnInfo),
    Username = maps:get(username, ClientInfo),
    Action = <<"disconnected">>,
    IsSuperuser = maps:get(is_superuser, ClientInfo),
    Online = 0,
    Payload = [
        {action, Action},
        {device_id, Username},
        {client_id, ClientId},
        {reason, ReasonCode},
        {ts, Ts},
        {online, Online}
    ],
    if 
        not IsSuperuser ->
            send_kafka(Payload, Username, <<"mqttdisconn">>);
        true -> ok
    end.

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
