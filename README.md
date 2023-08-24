# RSocket-py

Python implementation of [RSocket](http://rsocket.io)

| NOTE: The python api is not final and may be broken between minor versions<br/>See CHANGELOG for <b>Breaking Changes</b> |
|--------------------------------------------------------------------------------------------------------------------------------|

# Installation

The latest version from [pypi](https://pypi.org/project/rsocket/) can be installed using:

```shell
pip install rsocket
```

You may also install using some **extras**:

| Extra       | Functionality                       |
|-------------|-------------------------------------|
| rx          | ReactiveX (v3) integration          |
| reactivex   | ReactiveX (v4) integration          |
| aiohttp     | Websocket transport (server/client) |
| quart       | Websocket transport (server only)   |
| quic        | QUIC transport                      |
| cli         | Command line                        |
| optimized   | Frame parse/serialize optimizations |
| cloudevents | CloudEvents integration             |

```shell
pip install rsocket[reactivex]
```

Alternatively, download the source code, build a package:

```shell
python3 setup.py bdist_wheel
```

Use the resulting package from the **./dist** folder, or install locally:

```shell
python3 setup.py install
```

# Documentation

[Documentation](https://rsocket.io/guides/rsocket-py) is available on the official rsocket.io site.

# Examples

Examples can be found in the **/examples** folder. It contains various server and client usages. The following is a table
denoting which client example is constructed to be run against which server example. Some examples
are in java to show compatibility with a different implementation. To run the java examples first build using <code>mvn
package</code>.

The **examples/test_examples.py** can be used to execute the relevant example server/client pairs.

client_springboot.py is set up to work against the [Spring RSocket demo](https://github.com/benwilcock/spring-rsocket-demo).

| server (python)                    | server (java)           | client (python)                    | client(java)    |
|------------------------------------|-------------------------|------------------------------------|-----------------|
| server.py                          |                         | client.py                          |                 |
| server_quic.py                     |                         | client_quic.py                     |                 |
| server_with_lease.py               |                         |                                    | ClientWithLease |
| server_with_routing.py             |                         | client_with_routing.py             | Client          |
| server_with_routing.py             |                         | client_rx.py                       |                 |
| server_with_routing.py             |                         | client_reconnect.py                |                 |
|                                    | Server                  | run_against_example_java_server.py |                 |
|                                    | ServerWithFragmentation | client_with_routing.py             |                 |
| server_quart_websocket.py          |                         | client_websocket.py                |                 |
| server_aiohttp_websocket.py        |                         | client_websocket.py                |                 |

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
    - [X] Websocket (WS, WSS) - Quart and aiohttp integration
    - [X] QUIC
    - [X] HTTP/3
    - [ ] HTTP/2
    - [ ] Aeron
- [X] RxPy Integration
    - [X] Stream Response
    - [X] Channel Response
    - [X] Channel Requester stream
    - [X] Response
- [X] Other
    - [X] Reconnect
    - [X] Load balancing
    - [X] Server routing definition helper (Flask like syntax)
    - [X] Reactivex integration (v3, v4) server/client side
    - [X] Command line interface
