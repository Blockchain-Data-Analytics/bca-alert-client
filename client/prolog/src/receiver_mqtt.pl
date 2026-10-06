/*-------------------------------------------------------------------------*/
/* BCA Alerts Prolog client                                                */
/*                                                                         */
/* Copyright 2026 Alexander Diemand                                        */
/* SPDX-License-Identifier: Apache-2.0                                     */
/*                                                                         */
/* File  : receiver_mqtt.pl                                                */
/*                                                                         */
/* MQTT receiver: subscribes to bca/<Customer_id>/events on the BCA        */
/* broker and hands every message to the processor.                        */
/*                                                                         */
/* env. vars:                                                              */
/*   Token        (required) Cardano Alerts token, used as MQTT username   */
/*   Customer_id  (required)                                               */
/*   MQTT_host    (mqtt.blockchain-applied.com)                            */
/*   MQTT_port    (8883)                                                   */
/*   MQTT_tls     (true)                                                   */
/*   MQTT_cafile  (first CA bundle found in the usual system locations)    */
/*   MQTT_client_id (random; set it to keep a persistent session)          */
/*-------------------------------------------------------------------------*/

receiver_mqtt_run :-
  config_required('Token', Token),
  config_required('Customer_id', Cid),
  config_env('MQTT_host', 'mqtt.blockchain-applied.com', Host),
  config_integer('MQTT_port', 8883, Port),
  config_flag('MQTT_tls', true, Tls),
  config_concat(['bca/', Cid, '/events'], Topic),

  mqtt_conf_new(Conf),
  mqtt_conf_set(Conf, host, Host),
  mqtt_conf_set(Conf, port, Port),
  mqtt_conf_set(Conf, username, Token),
  % the broker authenticates by token only, the password is unused
  mqtt_conf_set(Conf, password, x),
  receiver_mqtt_session(Conf),
  receiver_mqtt_tls(Conf, Tls),

  log_msg("mqtt: connecting to ~a:~w~n", [Host, Port]),
  mqtt_client_new(Conf, Client),
  ( mqtt_connect(Client, 10000) -> true
  ; log_msg("mqtt: connection to ~a:~w failed~n", [Host, Port]),
    halt(1)
  ),
  ( mqtt_subscribe(Client, Topic, 1) -> true
  ; log_msg("mqtt: subscription to ~a failed~n", [Topic]),
    halt(1)
  ),
  log_msg("mqtt: subscribed to ~a~n", [Topic]),
  receiver_mqtt_loop(Client).

% a fixed client id with clean.session=false lets the broker keep QoS 1
% messages while this client is offline
receiver_mqtt_session(Conf) :-
  config_env('MQTT_client_id', Id), !,
  mqtt_conf_set(Conf, 'client.id', Id),
  mqtt_conf_set(Conf, 'clean.session', false).
receiver_mqtt_session(_).

receiver_mqtt_tls(_, false) :- !.
receiver_mqtt_tls(Conf, true) :-
  ( config_env('MQTT_cafile', CaFile) -> true
  ; receiver_mqtt_cafile(CaFile) -> true
  ; log_msg("mqtt: no CA bundle found, set MQTT_cafile~n", []),
    halt(2)
  ),
  mqtt_conf_set(Conf, 'tls.cafile', CaFile).

% system CA bundles (macOS, Debian/Ubuntu, Fedora/RHEL, Alpine/openSUSE, Homebrew)
receiver_mqtt_cafile(F) :-
  member(F, [ '/etc/ssl/cert.pem'
            , '/etc/ssl/certs/ca-certificates.crt'
            , '/etc/pki/tls/certs/ca-bundle.crt'
            , '/etc/ssl/ca-bundle.pem'
            , '/opt/homebrew/etc/openssl@3/cert.pem'
            ]),
  file_exists(F), !.

% failure driven, runs until the process is stopped. libmosquitto
% reconnects and renews the subscription on its own.
receiver_mqtt_loop(Client) :-
  repeat,
    ( mqtt_poll(Client, 1000, Payload, Meta) ->
        processor_handle(mqtt, Payload, Meta)
    ; true
    ),
    fail.
