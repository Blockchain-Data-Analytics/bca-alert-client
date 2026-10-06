/*-------------------------------------------------------------------------*/
/* BCA Alerts Prolog client                                                */
/*                                                                         */
/* Copyright 2026 Alexander Diemand                                        */
/* SPDX-License-Identifier: Apache-2.0                                     */
/*                                                                         */
/* File  : receiver_kafka.pl                                               */
/*                                                                         */
/* Kafka receiver: consumes bca.<Customer_id>.events from the BCA broker   */
/* (SASL_SSL, SCRAM-SHA-512) and hands every message to the processor.     */
/*                                                                         */
/* env. vars:                                                              */
/*   Customer_id     (required)                                            */
/*   Kafka_password  (required) SCRAM password handed out by BCA           */
/*   Kafka_user      (cust_<Customer_id>_kafka)                            */
/*   Kafka_brokers   (kafka.blockchain-applied.com:9093)                   */
/*   Kafka_group_id  (bca.<Customer_id>.prolog-client); the broker only    */
/*                   allows group ids starting with bca.<Customer_id>.     */
/*   Kafka_offset_reset (earliest) where a new group starts reading:       */
/*                   earliest replays the topic's retention, latest only   */
/*                   gets new events                                       */
/*   Kafka_security_protocol (SASL_SSL)                                    */
/*-------------------------------------------------------------------------*/

receiver_kafka_run :-
  config_required('Customer_id', Cid),
  config_required('Kafka_password', Password),
  config_concat(['cust_', Cid, '_kafka'], DefUser),
  config_env('Kafka_user', DefUser, User),
  config_env('Kafka_brokers', 'kafka.blockchain-applied.com:9093', Brokers),
  config_concat(['bca.', Cid, '.prolog-client'], DefGroup),
  config_env('Kafka_group_id', DefGroup, Group),
  config_env('Kafka_offset_reset', earliest, OffsetReset),
  config_env('Kafka_security_protocol', 'SASL_SSL', Protocol),
  config_concat(['bca.', Cid, '.events'], Topic),

  kafka_conf_new(Conf),
  kafka_conf_set(Conf, 'client.id', 'bca-alert-client-prolog'),
  kafka_conf_set(Conf, 'bootstrap.servers', Brokers),
  kafka_conf_set(Conf, 'group.id', Group),
  kafka_conf_set(Conf, 'auto.offset.reset', OffsetReset),
  kafka_conf_set(Conf, 'security.protocol', Protocol),
  receiver_kafka_sasl(Conf, Protocol, User, Password),

  log_msg("kafka: connecting to ~a as ~a, group ~a~n", [Brokers, User, Group]),
  ( kafka_consumer_new(Conf, Consumer) -> true
  ; log_msg("kafka: cannot create consumer~n", []),
    halt(1)
  ),
  ( kafka_subscribe(Consumer, [Topic]) -> true
  ; log_msg("kafka: subscription to ~a failed~n", [Topic]),
    halt(1)
  ),
  log_msg("kafka: subscribed to ~a~n", [Topic]),
  receiver_kafka_loop(Consumer).

% PLAINTEXT and SSL are for testing against a local broker only
receiver_kafka_sasl(Conf, Protocol, User, Password) :-
  memberchk(Protocol, ['SASL_SSL', 'SASL_PLAINTEXT']), !,
  kafka_conf_set(Conf, 'sasl.mechanisms', 'SCRAM-SHA-512'),
  kafka_conf_set(Conf, 'sasl.username', User),
  kafka_conf_set(Conf, 'sasl.password', Password),
  receiver_kafka_ssl(Conf, Protocol).
receiver_kafka_sasl(Conf, Protocol, _, _) :-
  receiver_kafka_ssl(Conf, Protocol).

% let librdkafka find the system CA certificates
receiver_kafka_ssl(Conf, Protocol) :-
  memberchk(Protocol, ['SASL_SSL', 'SSL']), !,
  kafka_conf_set(Conf, 'ssl.ca.location', probe).
receiver_kafka_ssl(_, _).

% failure driven, runs until the process is stopped. offsets are
% committed automatically by librdkafka (enable.auto.commit).
receiver_kafka_loop(Consumer) :-
  repeat,
    ( kafka_consumer_poll(Consumer, 1000, Payload, Meta) ->
        processor_handle(kafka, Payload, Meta)
    ; true
    ),
    fail.
