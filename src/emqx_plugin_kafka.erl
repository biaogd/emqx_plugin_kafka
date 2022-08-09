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
-export([ on_client_connect/3
        , on_client_connack/4
        , on_client_connected/3
        , on_client_disconnected/4
        , on_client_authenticate/3
        , on_client_check_acl/5
        , on_client_subscribe/4
        , on_client_unsubscribe/4
        ]).

%% Session Lifecircle Hooks
-export([ on_session_created/3
        , on_session_subscribed/4
        , on_session_unsubscribed/4
        , on_session_resumed/3
        , on_session_discarded/3
        , on_session_takeovered/3
        , on_session_terminated/4
        ]).

%% Message Pubsub Hooks
-export([ on_message_publish/2
        , on_message_delivered/3
        , on_message_acked/3
        , on_message_dropped/4
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
    emqx_hooks:add('client.connect',      {?MODULE, on_client_connect, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('client.connack',      {?MODULE, on_client_connack, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('client.connected',    {?MODULE, on_client_connected, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('client.disconnected', {?MODULE, on_client_disconnected, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('client.authenticate', {?MODULE, on_client_authenticate, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('client.check_acl',    {?MODULE, on_client_check_acl, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('client.subscribe',    {?MODULE, on_client_subscribe, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('client.unsubscribe',  {?MODULE, on_client_unsubscribe, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('session.created',     {?MODULE, on_session_created, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('session.subscribed',  {?MODULE, on_session_subscribed, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('session.unsubscribed',{?MODULE, on_session_unsubscribed, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('session.resumed',     {?MODULE, on_session_resumed, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('session.discarded',   {?MODULE, on_session_discarded, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('session.takeovered',  {?MODULE, on_session_takeovered, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('session.terminated',  {?MODULE, on_session_terminated, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('message.publish',     {?MODULE, on_message_publish, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('message.delivered',   {?MODULE, on_message_delivered, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('message.acked',       {?MODULE, on_message_acked, [Env]}, ?HP_HIGHEST),
    emqx_hooks:add('message.dropped',     {?MODULE, on_message_dropped, [Env]}, ?HP_HIGHEST).

%%--------------------------------------------------------------------
%% Client Lifecircle Hooks
%%--------------------------------------------------------------------

on_client_connect(ConnInfo, Props, _Env) ->
    %% this is to demo the usage of EMQX's structured-logging macro
    %% * Recommended to always have a `msg` field,
    %% * Use underscore instead of space to help log indexers,
    %% * Try to use static fields
    ?SLOG(debug, #{msg => "demo_log_msg_on_client_connect",
                   conninfo => ConnInfo,
                   props => Props}),
    {ok, Props}.

on_client_connack(ConnInfo = #{clientid := ClientId}, Rc, Props, _Env) ->
    io:format("Client(~s) connack, ConnInfo: ~p, Rc: ~p, Props: ~p~n",
              [ClientId, ConnInfo, Rc, Props]),
    {ok, Props}.

on_client_connected(ClientInfo = #{clientid := ClientId}, ConnInfo, _Env) ->
    io:format("Client(~s) connected, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
              [ClientId, ClientInfo, ConnInfo]).

on_client_disconnected(ClientInfo = #{clientid := ClientId}, ReasonCode, ConnInfo, _Env) ->
    io:format("Client(~s) disconnected due to ~p, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
              [ClientId, ReasonCode, ClientInfo, ConnInfo]).

on_client_authenticate(_ClientInfo = #{clientid := ClientId}, Result, _Env) ->
    io:format("Client(~s) authenticate, Result:~n~p~n", [ClientId, Result]),
    {ok, Result}.

on_client_check_acl(_ClientInfo = #{clientid := ClientId}, Topic, PubSub, Result, _Env) ->
    io:format("Client(~s) check_acl, PubSub:~p, Topic:~p, Result:~p~n",
              [ClientId, PubSub, Topic, Result]),
    {ok, Result}.

on_client_subscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
    io:format("Client(~s) will subscribe: ~p~n", [ClientId, TopicFilters]),
    {ok, TopicFilters}.

on_client_unsubscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
    io:format("Client(~s) will unsubscribe ~p~n", [ClientId, TopicFilters]),
    {ok, TopicFilters}.

%%--------------------------------------------------------------------
%% Session Lifecircle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) created, Session Info:~n~p~n", [ClientId, SessInfo]).

on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
    io:format("Session(~s) subscribed ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
    io:format("Session(~s) unsubscribed ~s with opts: ~p~n", [ClientId, Topic, Opts]).

on_session_resumed(#{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) resumed, Session Info:~n~p~n", [ClientId, SessInfo]).

on_session_discarded(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) is discarded. Session Info: ~p~n", [ClientId, SessInfo]).

on_session_takeovered(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) is takeovered. Session Info: ~p~n", [ClientId, SessInfo]).

on_session_terminated(_ClientInfo = #{clientid := ClientId}, Reason, SessInfo, _Env) ->
    io:format("Session(~s) is terminated due to ~p~nSession Info: ~p~n",
              [ClientId, Reason, SessInfo]).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    io:format("Publish ~s~n", [emqx_message:to_map(Message)]),
    {ok, Message}.

on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Env) ->
    ok;
on_message_dropped(Message, _By = #{node := Node}, Reason, _Env) ->
    io:format("Message dropped by node ~s due to ~s: ~p~n",
              [Node, Reason, emqx_message:to_map(Message)]).

on_message_delivered(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
    io:format("Message delivered to client(~s): ~p~n",
              [ClientId, emqx_message:to_map(Message)]),
    {ok, Message}.

on_message_acked(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
    io:format("Message acked by client(~s): ~p~n",
              [ClientId, emqx_message:to_map(Message)]).

%% Called when the plugin application stop
unload() ->
    emqx_hooks:del('client.connect',      {?MODULE, on_client_connect}),
    emqx_hooks:del('client.connack',      {?MODULE, on_client_connack}),
    emqx_hooks:del('client.connected',    {?MODULE, on_client_connected}),
    emqx_hooks:del('client.disconnected', {?MODULE, on_client_disconnected}),
    emqx_hooks:del('client.authenticate', {?MODULE, on_client_authenticate}),
    emqx_hooks:del('client.check_acl',    {?MODULE, on_client_check_acl}),
    emqx_hooks:del('client.subscribe',    {?MODULE, on_client_subscribe}),
    emqx_hooks:del('client.unsubscribe',  {?MODULE, on_client_unsubscribe}),
    emqx_hooks:del('session.created',     {?MODULE, on_session_created}),
    emqx_hooks:del('session.subscribed',  {?MODULE, on_session_subscribed}),
    emqx_hooks:del('session.unsubscribed',{?MODULE, on_session_unsubscribed}),
    emqx_hooks:del('session.resumed',     {?MODULE, on_session_resumed}),
    emqx_hooks:del('session.discarded',   {?MODULE, on_session_discarded}),
    emqx_hooks:del('session.takeovered',  {?MODULE, on_session_takeovered}),
    emqx_hooks:del('session.terminated',  {?MODULE, on_session_terminated}),
    emqx_hooks:del('message.publish',     {?MODULE, on_message_publish}),
    emqx_hooks:del('message.delivered',   {?MODULE, on_message_delivered}),
    emqx_hooks:del('message.acked',       {?MODULE, on_message_acked}),
    emqx_hooks:del('message.dropped',     {?MODULE, on_message_dropped}).
