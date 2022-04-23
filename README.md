# RSocket-py

Python implementation of [RSocket](http://rsocket.io)

# Installation

The [pypi](https://pypi.org/project/rsocket/) stable package (version 0.2) is very old.

Currently, the pre-release package can be installed using:

```shell
pip install --pre rsocket
```

or install any of the extras:
  * rx
  * aiohttp
  * quart
  * uic

Example:
```shell
pip install --pre rsocket[rx]
```

Alternatively, download the source code, build a package:

```shell
python3 setup.py bdist_wheel
```

and use the resulting package from the **./dist** folder, or install locally:

```shell
python3 setup.py install
```

# Documentation

[Documentation](https://rsocket.io/guides/rsocket-py) is available on the official rsocket.io site.

# Examples

Examples can be found in the /examples folder. It contains various server and client usages. The following is a table
denoting which <b>client</b> example is constructed to be run against which <b>server</b> example. Some of the examples
are in java to show compatibility with a different implementation.

The **examples/test_examples.py** shows which pairs of client/server work with each other, and can be used to execute
all the examples
(except for the client_springboot.py which is set up to work against https://github.com/benwilcock/spring-rsocket-demo)

| server (python)             | server (java) | client (python)                    | client(java)    |
|-----------------------------|---------------|------------------------------------|-----------------|
| server.py                   |               | client.py                          |                 |
| server_quic.py              |               | client_quic.py                     |                 |
| server_with_lease.py        |               |                                    | ClientWithLease |
| server_with_routing.py      |               | client_with_routing.py             | Client          |
| server_with_routing.py      |               | client_rx.py                       |                 |
| server_with_routing.py      |               | client_reconnect.py                |                 |
|                             | Server        | run_against_example_java_server.py |                 |
| server_quart_websocket.py   |               | client_websocket.py                |                 |
| server_aiohttp_websocket.py |               | client_websocket.py                |                 |

# Build Status

![build master](https://github.com/rsocket/rsocket-py/actions/workflows/python-package.yml/badge.svg?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/rsocket/rsocket-py/badge.svg?branch=master)](https://coveralls.io/github/rsocket/rsocket-py?branch=master)
[![CodeQL](https://github.com/rsocket/rsocket-py/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/rsocket/rsocket-py/actions/workflows/codeql-analysis.yml)

# Progress

- [X] Requests
    - [X] Fire and forget
    - [X] Response
    - [X] Stream
    - [X] Channel
    - [X] Metadata push
- [ ] Features
    - [X] Keepalive / Max server life
    - [X] Lease
    - [ ] Resume
    - [X] Fragmentation
- [X] Extensions
    - [X] Composite metadata
    - [X] Per Stream Mimetype
    - [X] Routing
    - [X] Authentication
- [ ] Transports
    - [X] TCP
    - [X] Websocket
    - [X] QUIC
    - [ ] HTTP/2
    - [ ] Aeron
- [X] RxPy Integration
    - [X] Stream Response
    - [X] Channel Response
    - [X] Channel Requester stream
    - [X] Response
- [ ] Other
    - [ ] Error handling all scenarios in the protocol spec
    - [X] Reconnect
    - [X] Load balancing
