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
/*   MQTT_watchdog  (1500) seconds without any message (event or           */
/*                  heartbeat) before the client exits with status 3;      */
/*                  0 disables it                                          */
/*-------------------------------------------------------------------------*/

:- dynamic(receiver_mqtt_last/1).
:- dynamic(receiver_mqtt_up/1).

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

% failure driven, runs until the process is stopped. pl_mqtt reconnects
% and renews the subscription on its own; the watchdog covers the cases
% where that does not happen (e.g. pl_mqtt v0.1.0, whose network thread
% ends silently after a TLS error, or a token that is no longer accepted).
receiver_mqtt_loop(Client) :-
  config_integer('MQTT_watchdog', 1500, Limit),
  real_time(T0),
  retractall(receiver_mqtt_last(_)),
  assertz(receiver_mqtt_last(T0)),
  retractall(receiver_mqtt_up(_)),
  assertz(receiver_mqtt_up(true)),
  repeat,
    ( mqtt_poll(Client, 1000, Payload, Meta) ->
        receiver_mqtt_seen,
        processor_handle(mqtt, Payload, Meta)
    ; true
    ),
    receiver_mqtt_watch(Client, Limit),
    fail.

receiver_mqtt_seen :-
  real_time(T),
  retractall(receiver_mqtt_last(_)),
  assertz(receiver_mqtt_last(T)).

% receiver_mqtt_watch(+Client, +Limit)
% logs connection changes. the server sends a heartbeat per network every
% 600 s, so a silence of Limit seconds (default 2.5 heartbeat intervals)
% means the channel is dead even if the connection looks up: exit with
% status 3 and let the supervisor (e.g. docker --restart) start afresh.
receiver_mqtt_watch(Client, Limit) :-
  ( mqtt_is_connected(Client) -> Up = true ; Up = false ),
  receiver_mqtt_up(Was),
  ( Up == Was -> true
  ; retractall(receiver_mqtt_up(_)),
    assertz(receiver_mqtt_up(Up)),
    ( Up == true -> log_msg("mqtt: connection restored~n", [])
    ; log_msg("mqtt: connection lost, waiting for reconnect~n", [])
    )
  ),
  receiver_mqtt_last(Last),
  real_time(Now),
  Silent is (Now - Last) // 1000,
  ( Limit > 0, Silent >= Limit ->
      log_msg("mqtt: watchdog: no message for ~ds (MQTT_watchdog=~d, connected=~a), exiting~n",
              [Silent, Limit, Up]),
      halt(3)
  ; true
  ).
