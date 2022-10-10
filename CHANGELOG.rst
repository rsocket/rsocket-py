Changelog
---------

v0.4.0
======

- Breaking change: Added ability to await fire_and_forget and push_metadata:
    - Both now return a future which resolves when the frame (or all fragments) finished sending.
- Fixed fragmentation implementation (misunderstood spec):
    - fragments after first now correctly PayloadFrame
    - fragment size now includes header and frame length.
    - Breaking change: init parameter change: fragment_size -> fragment_size_bytes. No need to modify existin values.
    - Added checking fragment size limit (minimum 64) as in java implementation
    - Updated examples
- Added reactivex (RxPy version 4) wrapper client

v0.3.0
======
Initial mostly complete implementation after long time from previous release (0.2.0)

v0.2.0
======
Legacy. Unknown history.
