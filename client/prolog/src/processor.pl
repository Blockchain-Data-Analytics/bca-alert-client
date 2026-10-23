/*-------------------------------------------------------------------------*/
/* BCA Alerts Prolog client                                                */
/*                                                                         */
/* Copyright 2026 Alexander Diemand                                        */
/* SPDX-License-Identifier: Apache-2.0                                     */
/*                                                                         */
/* File  : processor.pl                                                    */
/*                                                                         */
/* demo processor: decodes the envelopes delivered by a receiver and       */
/* writes them to stdout. this is the place to hook in a downstream        */
/* process. JSON handling: pl_toolbox/src/json.pl of prolog-utilities.     */
/*                                                                         */
/* envelopes (one per message, same on MQTT and Kafka):                    */
/*   {"type":"event","payload":{"id":..,"detected_at":..,"slot_no":..,     */
/*     "epoch_no":..,"block_time":..,"tx_hash":..,"address":..,            */
/*     "event_type":..,                                                    */
/*     "network":..,"confirmations":..,"data":{..}}}                       */
/*   {"type":"heartbeat","last_event_id":..,"channel_status":..,           */
/*     "network":..,"sent_at":..}                                          */
/*-------------------------------------------------------------------------*/

:- dynamic(processor_network/1).

% processor_init
% optional env. var. Network (mainnet, preprod, preview): all networks
% share one topic per customer, so messages of other networks are skipped
processor_init :-
  retractall(processor_network(_)),
  ( config_env('Network', N) -> assertz(processor_network(N)) ; true ).

% processor_handle(+Channel, +Payload, +Meta)
% never fails: a broken message must not stop the receiver loop
processor_handle(Channel, Payload, Meta) :-
  catch(processor_handle_(Channel, Payload, Meta), E,
        log_msg("processor: error ~q~n", [E])),
  !,
  flush_output.
processor_handle(_, _, _) :-
  flush_output.

processor_handle_(Channel, Payload, Meta) :-
  ( from_json(Payload, Msg) ->
      processor_message(Channel, Msg, Meta)
  ; processor_raw(Channel, Payload, Meta)
  ).

processor_message(_, Msg, _) :-
  processor_skip(Msg), !.
processor_message(Channel, Msg, _) :-
  processor_get(Msg, type, event),
  processor_get(Msg, payload, Ev), !,
  processor_event(Channel, Ev).
processor_message(Channel, Msg, _) :-
  processor_get(Msg, type, heartbeat), !,
  processor_heartbeat(Channel, Msg).
processor_message(Channel, Msg, _) :-
  format("~a unknown message: ", [Channel]),
  processor_write(Msg), nl.

% processor_skip(+Msg)
% the message belongs to another network than the one selected
processor_skip(Msg) :-
  processor_network(Want),
  processor_msg_network(Msg, Net),
  Net \== Want.

processor_msg_network(Msg, Net) :-
  processor_get(Msg, payload, Ev), processor_get(Ev, network, Net), !.
processor_msg_network(Msg, Net) :-
  processor_get(Msg, network, Net).

processor_event(Channel, Ev) :-
  processor_field(Ev, detected_at, At),
  processor_field(Ev, network, Net),
  processor_field(Ev, event_type, Type),
  processor_field(Ev, id, Id),
  processor_field(Ev, slot_no, Slot),
  % time of the block holding the tx; null for events from before it was added
  processor_field(Ev, block_time, BlockTime),
  processor_field(Ev, epoch_no, Epoch),
  processor_field(Ev, confirmations, Conf),
  processor_field(Ev, address, Addr),
  processor_field(Ev, tx_hash, Tx),
  format("~a [~a/~a] event ~a  id=~w slot=~w block_time=~w epoch=~w confirmations=~w~n",
         [At, Channel, Net, Type, Id, Slot, BlockTime, Epoch, Conf]),
  format("    address: ~a~n", [Addr]),
  format("    tx_hash: ~a~n", [Tx]),
  ( processor_get(Ev, data, Data) ->
      write('    data:    '), processor_write(Data), nl
  ; true
  ).

processor_heartbeat(Channel, Msg) :-
  processor_field(Msg, sent_at, At),
  processor_field(Msg, network, Net),
  processor_field(Msg, last_event_id, Last),
  processor_field(Msg, channel_status, Status),
  format("~a [~a/~a] heartbeat  last_event_id=~w channel_status=~w~n",
         [At, Channel, Net, Last, Status]).

processor_raw(Channel, Payload, Meta) :-
  format("~a not JSON: ~w  ~w~n", [Channel, Payload, Meta]).

% processor_field(+Object, +Key, -Text)
% field for display; '-' if missing or null
processor_field(Obj, Key, V) :-
  processor_get(Obj, Key, V), V \== null, !.
processor_field(_, _, '-').

% JSON terms as decoded by from_json/2 of prolog-utilities (pl_toolbox):
% objects are json([Key=Value, ...])
processor_get(json(Pairs), Key, V) :-
  memberchk(Key=V, Pairs).

processor_write(Json) :-
  to_json(Json, Text),
  write(Text).
