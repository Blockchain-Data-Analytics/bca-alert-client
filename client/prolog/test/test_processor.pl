/*-------------------------------------------------------------------------*/
/* BCA Alerts Prolog client -- processor tests (no broker needed)          */
/*                                                                         */
/* Copyright 2026 Alexander Diemand                                        */
/* SPDX-License-Identifier: Apache-2.0                                     */
/*                                                                         */
/*   $ make test                                                           */
/*                                                                         */
/* JSON itself is tested in prolog-utilities (pl_toolbox: make check).     */
/* exits with status 0 on success, 1 on failure.                          */
/*-------------------------------------------------------------------------*/

:- initialization(test).

test :-
  findall(Name, (test_case(Name, Goal), \+ catch(Goal, _, fail)), Failed),
  test_samples,
  ( Failed == [] -> format("PASSED~n", []), halt(0)
  ; format("FAILED: ~w~n", [Failed]), halt(1)
  ).

test_event('{"type":"event","payload":{"id":423726482049734665,"detected_at":"2026-09-08T14:55:01.123+00:00","slot_no":197313019,"epoch_no":654,"tx_hash":"5f2c0a","address":"addr1q8055xyz","event_type":"utxo_received","network":"mainnet","confirmations":null,"data":{"lovelace":"1500000","assets":{},"output_index":0}}}').
test_heartbeat('{"type":"heartbeat","last_event_id":423726482049734665,"channel_status":"ok","network":"preprod","sent_at":"2026-09-08T15:05:00+00:00"}').

test_case(event, (
  test_event(E),
  test_written(processor_handle(mqtt, E, []), W),
  W == '2026-09-08T14:55:01.123+00:00 [mqtt/mainnet] event utxo_received  id=423726482049734665 slot=197313019 epoch=654 confirmations=-\n    address: addr1q8055xyz\n    tx_hash: 5f2c0a\n    data:    {"lovelace":"1500000","assets":{},"output_index":0}\n' )).
test_case(heartbeat, (
  test_heartbeat(H),
  test_written(processor_handle(kafka, H, []), W),
  W == '2026-09-08T15:05:00+00:00 [kafka/preprod] heartbeat  last_event_id=423726482049734665 channel_status=ok\n' )).
test_case(not_json, (
  test_written(processor_handle(mqtt, 'not json', []), W),
  W == 'mqtt not JSON: not json  []\n' )).
test_case(unknown_type, (
  test_written(processor_handle(mqtt, '{"type":"other","x":[true,null]}', []), W),
  W == 'mqtt unknown message: {"type":"other","x":[true,null]}\n' )).
% a datum integer beyond GNU Prolog's max_integer must not lose the event
test_case(big_datum_integer, (
  test_written(processor_handle(mqtt,
    '{"type":"event","payload":{"event_type":"rollback","data":{"int":123456789012345678901234567890}}}', []), W),
  atom_codes(W, Cs), atom_codes('event rollback', Sub),
  append(_, Rest, Cs), append(Sub, _, Rest), ! )).
test_case(network_filter, (
  assertz(processor_network(preprod)),
  from_json('{"type":"heartbeat","network":"mainnet"}', H1),
  from_json('{"type":"heartbeat","network":"preprod"}', H2),
  from_json('{"type":"event","payload":{"network":"mainnet"}}', E1),
  ( processor_skip(H1), \+ processor_skip(H2), processor_skip(E1) -> R = ok ; R = ko ),
  retractall(processor_network(_)),
  R == ok )).

test_written(Goal, Atom) :-
  open('/tmp/bca_alert_client_test.out', write, S),
  current_output(Old), set_output(S),
  call(Goal),
  set_output(Old), close(S),
  open('/tmp/bca_alert_client_test.out', read, R),
  read_codes(R, Cs), close(R),
  atom_codes(Atom, Cs).

read_codes(S, Cs) :-
  get_code(S, C),
  ( C =:= -1 -> Cs = [] ; Cs = [C|T], read_codes(S, T) ).

% shows what the processor prints for the envelopes push_worker sends
test_samples :-
  format("--- sample output ---~n", []),
  test_event(E), processor_handle(mqtt, E, [topic('bca/12345/events'), qos(1)]),
  test_heartbeat(H), processor_handle(kafka, H, [partition(0), offset(7)]),
  processor_handle(mqtt, 'not json', []),
  format("---------------------~n", []).
