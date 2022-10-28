Changelog
---------

v0.4.3
======
- Command line fixes:
    - limit_rate argument was effectively ignored. fixed.
- Added on_ready callback to RSocketServer. Called when sender/receiver tasks are ready.
- Implement ReactiveX (3.0, 4.0) server side handler. Allows to define RequestHandler directly using ReactiveX.
- Added sending_done_event argument to request_channel to allow client to wait until sending to server is complete/canceled

v0.4.2
======
- Command line fixes:
    - Support passing ssl certificate and http headers when using ws/wss
    - Support requesting --version without the need to specify URI arguments
    - Option --interactionModel to specify interaction (eg. request_response, request_stream)
    - Added Metadata Push support

v0.4.1
======
- Added running tests on python 3.11 and package classification
- Removed data and metadata content from logs. Replaced with data and metadata sizes
- Performance test examples available in *performance* folder
- WSS (Secure websocket) example and support (aiohttp)
    - Refactored Websocket transport to allow providing either url or an existing websocket
- Added command line tool (rsocket-py)

v0.4.0
======

- Breaking change: Added ability to await fire_and_forget and push_metadata:
    - Both now return a future which resolves when the payload was sent completely (including fragmentation for fnf)
- Fixed fragmentation implementation (misunderstood spec):
    - fragments after first one are now correctly of type PayloadFrame
    - fragment size now includes frame header and length
    - Added checking fragment size limit (minimum 64) as in java implementation
    - Updated examples
- Added reactivex (RxPy version 4) wrapper client
- Added Initial support for http3 (wss)
- Better type hint for return value of request_response

v0.3.0
======
Initial mostly complete implementation after long time from previous release (0.2.0)

v0.2.0
======
Legacy. Unknown history.
