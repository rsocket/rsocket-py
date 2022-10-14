Changelog
---------

v0.4.0
======

- Breaking change: Added ability to await fire_and_forget and push_metadata:
    - Both now return a future which resolves when the frame (or all fragments) finished sending.
- Fixed fragmentation implementation (misunderstood spec):
    - fragments after first one are now correctly of type PayloadFrame
    - fragment size now includes frame header and length.
    - Added checking fragment size limit (minimum 64) as in java implementation
    - Updated examples

v0.3.0
======
Initial mostly complete implementation after long time from previous release (0.2.0)

v0.2.0
======
Legacy. Unknown history.
