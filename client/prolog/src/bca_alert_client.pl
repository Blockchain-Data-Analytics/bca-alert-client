/*-------------------------------------------------------------------------*/
/* BCA Alerts Prolog client                                                */
/*                                                                         */
/* Copyright 2026 Alexander Diemand                                        */
/* SPDX-License-Identifier: Apache-2.0                                     */
/*                                                                         */
/* File  : bca_alert_client.pl                                             */
/*                                                                         */
/* entry point: bca-alert-client mqtt|kafka                                */
/* events go to stdout, diagnostics to stderr.                             */
/*-------------------------------------------------------------------------*/

:- initialization(main).

main :-
  argument_list(Args),
  ( Args = [Channel], main_receiver(Channel, Run) ->
      processor_init,
      call(Run)
  ; main_usage,
    halt(2)
  ).

main_receiver(mqtt, receiver_mqtt_run).
main_receiver(kafka, receiver_kafka_run).

main_usage :-
  member(L, [ 'usage: bca-alert-client mqtt|kafka'
            , ''
            , 'environment:'
            , '  Customer_id      your Cardano Alerts customer id (both channels)'
            , '  Token            your Cardano Alerts token (mqtt)'
            , '  Kafka_password   your Kafka password (kafka)'
            , '  Network          optional: only show mainnet, preprod or preview'
            , 'see README.md for optional settings'
            ]),
    format(user_error, "~a~n", [L]),
    fail.
main_usage.
