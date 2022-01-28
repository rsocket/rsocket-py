# RSocket-py

Python implementation of [RSocket](http://rsocket.io)

# Build Status

![build master](https://github.com/rsocket/rsocket-py/actions/workflows/python-package.yml/badge.svg?branch=master)

# Progress

- [X] Requests
    - [X] Fire and forget
    - [X] Response
    - [X] Stream
    - [X] Cahnnel
- [ ] Features
    - [X] Metadata push
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
    - [ ] Websocket
    - [ ] HTTP/2
    - [ ] Aeron
- [ ] RxPy Integration
    - [X] Stream Response
    - [X] Channel Response
    - [ ] Channel Requester stream
    - [ ] Response
- [ ] Other
    - [ ] Error handling all scenarios in the protocol spec
