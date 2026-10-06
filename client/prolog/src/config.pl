/*-------------------------------------------------------------------------*/
/* BCA Alerts Prolog client                                                */
/*                                                                         */
/* Copyright 2026 Alexander Diemand                                        */
/* SPDX-License-Identifier: Apache-2.0                                     */
/*                                                                         */
/* File  : config.pl                                                       */
/*                                                                         */
/* configuration from environment variables. secrets (Token,               */
/* Kafka_password) are only read here and never printed.                   */
/*-------------------------------------------------------------------------*/

% config_env(+Name, -Value)
% the variable is set and not empty
config_env(Name, Value) :-
  environ(Name, Value),
  Value \== ''.

% config_env(+Name, +Default, -Value)
config_env(Name, _, Value) :-
  config_env(Name, Value), !.
config_env(_, Default, Default).

% config_required(+Name, -Value)
% stops the program with exit status 2 if the variable is missing
config_required(Name, Value) :-
  config_env(Name, Value), !.
config_required(Name, _) :-
  log_msg("error: environment variable ~a is not set~n", [Name]),
  halt(2).

% config_flag(+Name, +Default, -Bool)
% true/false from 1/0, true/false, yes/no, on/off
config_flag(Name, Default, Bool) :-
  config_env(Name, Default, V),
  ( memberchk(V, ['1', true, yes, on]) -> Bool = true ; Bool = false ).

% config_integer(+Name, +Default, -Int)
config_integer(Name, _, Int) :-
  config_env(Name, V), !,
  ( catch((atom_codes(V, Cs), number_codes(Int, Cs)), _, fail), integer(Int) -> true
  ; log_msg("error: environment variable ~a must be an integer, got '~a'~n", [Name, V]),
    halt(2)
  ).
config_integer(_, Default, Default).

% log_msg(+Format, +Args)
% diagnostics go to stderr, so stdout carries only the processor's output
log_msg(Format, Args) :-
  format(user_error, Format, Args),
  flush_output(user_error).

% config_concat(+Atoms, -Atom)
config_concat([], '').
config_concat([A|As], R) :-
  config_concat(As, R0),
  atom_concat(A, R0, R).
