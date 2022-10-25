Changelog
---------


v0.4.1
======
- Added running tests on python 3.11 and package classification
- Removed data and metadata content from logs. Replaced with data and metadata sizes
- Performance test examples available in *performance* folder

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
