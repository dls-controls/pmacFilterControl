import asyncio

import aiozmq
import pytest
import zmq
from mock import AsyncMock, Mock

from pmacfiltercontrol.zmqadapter import ZeroMQAdapter

# -------------------------------------------------


# https://www.roguelynn.com/words/asyncio-testing/
# used to patch/mock asyncio coroutines
@pytest.fixture
def create_mock_coro(mocker, monkeypatch):
    def _create_mock_patch_coro(to_patch=None):
        mock = mocker.Mock()

        async def _coro(*args, **kwargs):
            return mock(*args, **kwargs)

        if to_patch:  # <-- may not need/want to patch anything
            monkeypatch.setattr(to_patch, _coro)
        return mock, _coro

    return _create_mock_patch_coro


# -------------------------------------------------
# Mock Queue


@pytest.fixture
def mock_queue(mocker, monkeypatch):
    queue = mocker.AsyncMock()
    monkeypatch.setattr(asyncio, "Queue", queue)
    return queue.return_value


@pytest.fixture
def mock_queue_get(mock_queue, create_mock_coro):
    mock_get, coro_get = create_mock_coro()
    mock_queue.get = coro_get
    return mock_get


@pytest.fixture
def mock_queue_put_nowait(mock_queue, create_mock_coro):
    mock_put_nowait, coro_put_nowait = create_mock_coro()
    mock_queue.put_nowait = coro_put_nowait
    return mock_put_nowait


# -------------------------------------------------
# Mock Socket


@pytest.fixture
def mock_socket(mocker, monkeypatch):
    socket = mocker.AsyncMock()
    monkeypatch.setattr(aiozmq, "ZmqStream", socket)
    return socket.return_value


@pytest.fixture
def mock_socket_read(mock_socket, create_mock_coro):
    mock_read, coro_read = create_mock_coro()
    mock_socket.read = coro_read
    return mock_read


@pytest.fixture
def mock_socket_write(mock_socket, create_mock_coro):
    mock_write, coro_write = create_mock_coro()
    mock_socket.write = coro_write
    return mock_write


# -------------------------------------------------
# Zmq Adapter


@pytest.fixture
def zmq_adapter() -> ZeroMQAdapter:
    zmq_adapter = ZeroMQAdapter(zmq_type=zmq.SUB)
    return zmq_adapter


@pytest.fixture
def zmq_adapter_dealer() -> ZeroMQAdapter:
    zmq_adapter = ZeroMQAdapter(zmq_type=zmq.DEALER)
    return zmq_adapter


# -------------------------------------------------
# Tests


def test_zmq_adapter_constructor():
    ZeroMQAdapter()


@pytest.mark.asyncio
async def test_start_stop_stream(zmq_adapter: ZeroMQAdapter):
    await zmq_adapter.start_stream()
    assert zmq_adapter._socket._closing is False

    await zmq_adapter.close_stream()
    # wait for socket to close
    await asyncio.sleep(0.1)
    assert zmq_adapter._socket._closing is True


def test_zmq_adapter_if_running(zmq_adapter: ZeroMQAdapter):
    assert zmq_adapter.check_if_running() is False


def test_zmq_adapter_send_message(zmq_adapter: ZeroMQAdapter):
    mock_message = AsyncMock()

    zmq_adapter._send_message_queue = Mock(asyncio.Queue)

    zmq_adapter.send_message(mock_message)


@pytest.mark.asyncio
async def test_zmq_adapter_get_response(
    zmq_adapter: ZeroMQAdapter, mock_queue, mock_queue_get
):
    zmq_adapter._recv_message_queue = mock_queue
    mock_queue_get.return_value = b"test"

    resp = await zmq_adapter.get_response()

    assert resp == b"test"

    mock_queue_get.assert_called_once()


@pytest.mark.asyncio
async def test_zmq_adapter_read_response(
    zmq_adapter: ZeroMQAdapter, mock_socket, mock_socket_read
):
    zmq_adapter._socket = mock_socket

    f: asyncio.Future = asyncio.Future()
    f.set_result([b"test"])
    mock_socket_read.return_value = f.result()

    resp = await zmq_adapter._read_response()
    assert resp == b"test"

    mock_socket_read.assert_called_once()


@pytest.mark.asyncio
async def test_zmq_adapter_dealer_read_response(
    zmq_adapter_dealer: ZeroMQAdapter, mock_socket, mock_socket_read
):
    zmq_adapter_dealer._socket = mock_socket

    f: asyncio.Future = asyncio.Future()
    f.set_result([b"", b"test"])
    mock_socket_read.return_value = f.result()

    resp = await zmq_adapter_dealer._read_response()

    assert resp == b"test"

    mock_socket_read.assert_called_once()


@pytest.mark.asyncio
async def test_zmq_adapter_run_forever(zmq_adapter: ZeroMQAdapter):
    zmq_adapter._process_response_queue = AsyncMock()

    await zmq_adapter.run_forever()

    # zmq_adapter._process_response_queue.assert_awaited_once()


@pytest.mark.asyncio
async def test_zmq_adapter_dealer_run_forever(zmq_adapter_dealer: ZeroMQAdapter):
    zmq_adapter_dealer._process_message_queue = AsyncMock()
    zmq_adapter_dealer._process_response_queue = AsyncMock()

    await zmq_adapter_dealer.run_forever()

    # zmq_adapter_dealer._process_message_queue.assert_awaited_once()
    # zmq_adapter_dealer._process_response_queue.assert_awaited_once()


@pytest.mark.asyncio
async def test_zmq_adapter_process_message_queue(
    zmq_adapter: ZeroMQAdapter, mock_queue, mock_queue_get
):
    zmq_adapter._process_message = AsyncMock()

    zmq_adapter._send_message_queue = mock_queue
    mock_queue_get.return_value = b"test"

    await zmq_adapter._process_message_queue()

    mock_queue_get.assert_called_once()


@pytest.mark.asyncio
async def test_zmq_adapter_process_message_no_messsage(
    zmq_adapter: ZeroMQAdapter, capsys
):
    await zmq_adapter._process_message(None)

    captured = capsys.readouterr()
    assert captured.out == "No message\n"


@pytest.mark.asyncio
async def test_zmq_adapter_process_message_socket_closed(
    zmq_adapter: ZeroMQAdapter, mock_socket, capsys
):
    zmq_adapter._socket = mock_socket
    zmq_adapter._socket._closing = True

    await zmq_adapter._process_message([b"test"])

    captured = capsys.readouterr()
    assert captured.out == "Socket closed...\n"


@pytest.mark.asyncio
async def test_zmq_adapter_process_message_zmq_error(
    zmq_adapter: ZeroMQAdapter, mock_socket, mock_socket_write, capsys
):
    zmq_adapter._socket = mock_socket
    zmq_adapter._socket._closing = False

    # Shouldn't need to specify this below
    zmq_adapter._socket.write = mock_socket_write
    mock_socket_write.side_effect = zmq.error.ZMQError(6)

    await zmq_adapter._process_message([b"test"])

    captured = capsys.readouterr()
    assert captured.out == "ZMQ Error No such device or address\n"


@pytest.mark.asyncio
async def test_zmq_adapter_process_message_exception(
    zmq_adapter: ZeroMQAdapter, mock_socket, mock_socket_write, capsys
):
    zmq_adapter._socket = mock_socket
    zmq_adapter._socket._closing = False

    # Shouldn't need to specify this below
    zmq_adapter._socket.write = mock_socket_write
    mock_socket_write.side_effect = Exception("Test Exception")

    await zmq_adapter._process_message([b"test"])

    captured = capsys.readouterr()
    assert (
        captured.out == "Error, Test Exception\n"
        "Unable to write to ZMQ stream, trying again...\n"
    )


@pytest.mark.asyncio
async def test_zmq_adapter_process_message_sub(
    zmq_adapter: ZeroMQAdapter, mock_socket, mock_socket_write
):
    zmq_adapter._socket = mock_socket
    # Shouldn't need to specify this below
    zmq_adapter._socket.write = mock_socket_write
    zmq_adapter._socket._closing = False

    await zmq_adapter._process_message([b"test"])

    mock_socket_write.assert_called_once()


@pytest.mark.asyncio
async def test_zmq_adapter_process_message_dealer(
    zmq_adapter_dealer: ZeroMQAdapter, mock_socket, mock_socket_write
):
    zmq_adapter_dealer._socket = mock_socket
    # Shouldn't need to specify this below
    zmq_adapter_dealer._socket.write = mock_socket_write
    zmq_adapter_dealer._socket._closing = False

    await zmq_adapter_dealer._process_message([b"test"])

    mock_socket_write.assert_called_once()


@pytest.mark.asyncio
async def test_zmq_adapter_process_response_queue(
    zmq_adapter: ZeroMQAdapter, mock_queue, mock_queue_put_nowait
):
    zmq_adapter._read_response = AsyncMock()
    zmq_adapter._read_response.return_value = b"test"

    zmq_adapter._recv_message_queue = mock_queue
    # Shouldn't need to specify this below
    zmq_adapter._recv_message_queue.put_nowait = mock_queue_put_nowait

    await zmq_adapter._process_response_queue()

    mock_queue_put_nowait.assert_called_once()
