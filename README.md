# RSocket-py

Python implementation of [RSocket](http://rsocket.io)

# Documentation

[Documentation](https://rsocket.io/guides/rsocket-py) is available on the official rsocket.io site.

# Examples

Examples can be found in the /examples folder. It contains various server and client usages. The following is a table
denoting which <b>client</b> example is constructed to be run against which <b>server</b> example. Some of the examples
are in java to show compatibility with a different implementation.

| server (python)        | server (java) | client (python)                    | client(java)    |
|------------------------|---------------|------------------------------------|-----------------|
| server.py              |               | client.py                          |                 |
| server_with_lease.py   |               |                                    | ClientWithLease |
| server_with_routing.py |               | client_with_routing.py             | Client          |
|                        | Server        | run_against_example_java_server.py |                 |

# Build Status

![build master](https://github.com/rsocket/rsocket-py/actions/workflows/python-package.yml/badge.svg?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/rsocket/rsocket-py/badge.svg?branch=master)](https://coveralls.io/github/rsocket/rsocket-py?branch=master)

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
    - [ ] Per Stream Mimetype
    - [X] Routing
    - [X] Authentication
- [ ] Transports
    - [X] TCP
    - [X] Websocket
    - [ ] HTTP/2
    - [ ] Aeron
- [X] RxPy Integration
    - [X] Stream Response
    - [X] Channel Response
    - [X] Channel Requester stream
    - [X] Response
- [ ] Other
    - [ ] Error handling all scenarios in the protocol spec
