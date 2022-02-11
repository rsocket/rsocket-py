import pytest

from rsocket.exceptions import RSocketStreamAllocationFailure
from rsocket.stream_control import StreamControl


@pytest.mark.parametrize('first_stream_id', (1, 2))
def test_stream_control_first_allocated_stream(first_stream_id):
    control = StreamControl(first_stream_id)

    assert control.allocate_stream() == first_stream_id


@pytest.mark.parametrize('first_stream_id', (1, 2))
def test_stream_control_allocate_relevant_streams(first_stream_id):
    control = StreamControl(first_stream_id)

    for i in range(3000):
        assert control.allocate_stream() % 2 == first_stream_id % 2


@pytest.mark.parametrize('first_stream_id', (1, 2))
def test_stream_control_raise_exception_on_no_streams_available(first_stream_id):
    control = StreamControl(first_stream_id)
    maximum_stream_id = 0x7F
    control._maximum_stream_id = maximum_stream_id
    dummy_stream = object()

    for i in range(first_stream_id, maximum_stream_id + 1, 2):  # fill all streams with dummy
        control.register_stream(i, dummy_stream)

    with pytest.raises(RSocketStreamAllocationFailure):
        for i in range(maximum_stream_id):
            control.allocate_stream()


def test_stream_control_reuse_old_stream_ids():
    control = StreamControl(1)
    maximum_stream_id = 0x7F
    control._maximum_stream_id = maximum_stream_id
    control._current_stream_id = 15
    dummy_stream = object()

    for i in range(1, maximum_stream_id + 1, 2):  # fill all streams with dummy
        control.register_stream(i, dummy_stream)

    control.finish_stream(5)

    next_stream = control.allocate_stream()

    assert next_stream == 5
