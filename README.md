# BCA Alert clients

## License

Copyright 2026 Alexander Diemand

Licensed under the [Apache License, Version 2.0](./LICENSE) (SPDX-License-Identifier: `Apache-2.0`), see also [NOTICE](./NOTICE).

The clients build on third-party libraries under their own licenses (several are GPL-3.0-or-later); each client lists them in its own README and NOTICE, e.g. [client/prolog](client/prolog/README.md#license).

## Overview

1. choose blockchain (Cardano, Bitcoin, others)
2. register search criteria like address
3. the agent will regularly search the blockchain for hits
4. on a hit: stream out the information found on channels (webhook, MQTT, Kafka)

## Implementations

The demo client is implemented in several languages:

|language|webhook|MQTT|Kafka|API|
|----|----|----|----|----|
|C++ |  -  |  -  |  -  |  -  |
|Go  |  -  |  -  |  -  |  -  |
|Rust |  -  |  -  |  -  |  -  |
|Python |  -  |  -  |  -  |  -  |
|[Prolog](client/prolog/) |  -  |  ✓  |  ✓  |  -  |

### architecture

The client consists of two modules: Receiver and Processor

    * Receiver: configures from env. var. $Token and $Customer_id; opens connection to the BCA site (mqtt/kafka); listens for events
    * Processor: demo implementation of outputting the received events to a downstream process; here it only outputs to the console

#### Channel selection

The receiver exists in two instances: one for MQTT and one for Kafka connections.

The channel `mqtt` or `kafka` can be chosen from the command line (see EXAMPLES).

## Disclaimer

The clients in this repository are demo implementations showing how to
receive BCA alerts; they are not meant for production use as is. They are
provided "AS IS", without warranties or conditions of any kind (see
sections 7 and 8 of the [license](./LICENSE)).

Events can be missed or delivered more than once, e.g. while a client is
not running or reconnecting. Do not rely on a client alone for decisions
involving funds; reconcile against the BCA API or the blockchain itself.
The output is information, not financial advice.

Parts of this code have been produced with the help of AI; see the notes
of each client.
