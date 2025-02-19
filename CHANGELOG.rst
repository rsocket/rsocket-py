Changelog
---------

v0.4.17
=======
- python 3.13 support
- Documentation and example code update

v0.4.16
=======
- Fix using router request-response with reactivex response
- Fix examples of consuming the payload of a request-channel
- Fix the reactivex example of the tutorial application

v0.4.15
=======
- Websockets server support (https://github.com/python-websockets/websockets)
- AsyncWebsockets client support (https://github.com/Fuyukai/asyncwebsockets)

v0.4.14
=======
- GraphQL: Use async methods for query resolvers. Fixed using mutation methods and passing variables.

v0.4.13
=======
- GraphQL basic support (See examples/graphql)

v0.4.12
=======
- Fixed fragmentation for fire and forget
- Fixed incorrect limit on tag (and route) length from 127 to 255 (GooDer)

v0.4.11
=======
- Breaking change: RequestRouter argument 'payload_mapper' was replaced with 'payload_deserializer' and 'payload_serializer'
- Added CloutEvent serialize/deserialize helpers for use in RequestRouter

v0.4.10
=======
- Code cleanup
- Breaking change: Removed deprecated rsocket.routing.helpers module
- Added CloudEvents client/server usage example (compatible with java rsocket example from cloudevents/sdk-java)
- Switched to pyproject.toml

v0.4.9
======
- Optimization to routing requests to methods and parsing composite metadata

v0.4.8
======
- Removed copying data and metadata into frame during serialization

v0.4.7
======
- Send **cancel** to responders when requester disconnects and **error** to requesters when requester disconnects
- Fix guide examples to properly cancel responders which use asyncio Task as value source
- Rewrote guide statistics example to use generator instead of task

v0.4.6
======
- fire_and_forget now only removes the stream id when the future denoting the frame was sent, is done
- API documentation auto generated at rsocket.readthedocs.io
- Request router changes:
    - Raise error on empty or None route specified in request router
    - Added the following methods to RequestRouter to allow specifying handlers of unknown routes:
        - response_unknown
        - stream_unknown
        - channel_unknown
        - fire_and_forget_unknown
        - metadata_push_unknown
    - Officially support route aliases by using the decorator multiple times on the same method
    - Fix value mapping in request router:
        -A parameter of any name (not just *payload*) specified on a routed method with a type-hint other than Payload will use the payload_mapper to decode the value
        - Any parameter with the type CompositeMetadata will receive the composite metadata

v0.4.5
======
- Breaking change: Normalized the request_channel method argument names across implementations and added where missing (vanilla, reactivex etc.):
    - **local_publisher** renamed to **publisher**
    - **sending_done_event** renamed to **sending_done**
- Breaking change: ReactiveX clients will remove empty payload from request_response Observable, resulting in an actually empty Observable
- Bug fix: fixed channel stream being released prematurely when canceled by requester, and responder side still working
- Bug fix: removed cyclic references in RSocketBase which caused old sessions not to be released
- Bug fix: fixed ability for ReactiveX streams and fragmented responses to send payloads concurrently
- CollectorSubscriber : exposed subscription methods directly instead of relying on internal **subscription** variable
- Reactivex server side request_response allowed to return reactivex.empty(). Library code will replace with empty Payload when needed
- Added EmptyStream for use in stream and channel responses
- Tutorial code: release logged out users from global chat data (weak references)

v0.4.4
======
- Fragmentation fix - empty payload (either in request or response) with fragmentation enabled failed to send
- Breaking change: *on_connection_lost* was renamed to *on_close*. An *on_connection_error* method was added to handle initial connection errors
- Routing request handler:
    - Throws an RSocketUnknownRoute exception which results in an error frame on the requester side
    - Added error logging for response/stream/channel requests
- Added *create_response* helper method as shorthand for creating a future with a Payload
- Added *utf8_decode* helper. Decodes bytes to utf-8. If data is None, returns None.
- Refactoring client reconnect flow
- Added example code for tutorial on rsocket.io

v0.4.3
======
- Command line fixes:
    - limit_rate argument was effectively ignored. fixed
- Added on_ready callback to RSocketServer. Called when sender/receiver tasks are ready
- Implement ReactiveX (3.0, 4.0) server side handler. Allows to define RequestHandler directly using ReactiveX
- Added sending_done_event argument to request_channel to allow client to wait until sending to server is complete/canceled
- Added find_by_mimetype to CompositeMetadata class. Returns list of relevant items by mimetype
- Breaking Change: Removed RSocketBase class dependency from RequestHandler. It is not longer required as an argument to __init__

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
