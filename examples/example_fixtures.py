from rsocket.frame_helpers import ensure_bytes

large_data1 = b''.join(ensure_bytes(str(i)) + b'123456789' for i in range(50))
