import asyncio

# from importlib import reload
# from pathlib import Path
from typing import Dict

import pytest
from mock import Mock
from mock.mock import create_autospec

from pmacfiltercontrol.hdfadapter import HDFAdapter

# from pmacfiltercontrol.pmacFilterControlWrapper import Wrapper
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
# Mock Socket


@pytest.fixture
def mock_run_coroutine_threadsafe(mocker, monkeypatch):
    _run_coroutine_threadsafe = mocker.Mock()
    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", _run_coroutine_threadsafe)
    return _run_coroutine_threadsafe.return_value


# -------------------------------------------------
# Objects


@pytest.fixture
def mock_zmq_adapter() -> Mock:
    return create_autospec(ZeroMQAdapter)


@pytest.fixture
def mock_hdf_adapter() -> Mock:
    return create_autospec(HDFAdapter)


# @pytest.fixture
# def builder_fixture():
#     from softioc import builder, pythonSoftIoc

#     reload(pythonSoftIoc)
#     # reload(device)
#     builder = reload(builder)
#     return builder


# @pytest.fixture
# def pfc_ioc(builder_fixture) -> Wrapper:
#     test_wrapper = Wrapper(
#         "127.0.0.1",
#         9000,
#         9001,
#         builder=builder_fixture,
#         device_name="pytest_pfcw",
#         filter_set_total=2,
#         filters_per_set=2,
#         detector="BLXXI-TEST-EXCBR-01",
#         motors="BLXXI-TEST-FILT-01",
#         autosave_file_path=f"{Path.cwd()}/tests/test_autosave.txt",
#         hdf_file_path=f"{Path.cwd()}/tests/",
#     )
#     return test_wrapper


# -------------------------------------------------


# def test_pfc_ioc_constructor(builder_=builder):
#     # Don't want these to be called so made into Mocks
#     Wrapper._generate_filter_pos_records = Mock()
#     Wrapper._generate_shutter_records = Mock()
#     Wrapper._generate_pixel_threshold_records = Mock()

#     test_w = Wrapper(
#         "127.0.0.1",
#         9998,
#         9999,
#         builder=builder_,
#         device_name="test_test",
#         filter_set_total=2,
#         filters_per_set=2,
#         detector="BLXXI-TEST-EXCBR-01",
#         motors="BLXXI-TEST-FILT-01",
#         autosave_file_path=f"{Path.cwd()}/tests/test_autosave.txt",
#         hdf_file_path=f"{Path.cwd()}/tests/",
#     )

#     assert test_w.ip == "127.0.0.1"
#     assert test_w.timeout.get() == 3


@pytest.mark.asyncio
async def test_pfc_ioc_send_initial_config(pfc_ioc, mock_run_coroutine_threadsafe):
    pfc_ioc.connected = True
    pfc_ioc._autosave_dict = {"pytest_pfcw:FILTER_SET": 1}

    pfc_ioc._configure_param = Mock()
    pfc_ioc._setup_hist_thresholds = Mock()
    pfc_ioc.shutter_pos_closed.get = Mock()
    pfc_ioc._set_filter_set = Mock()

    await pfc_ioc._send_initial_config()

    assert pfc_ioc.attenuation.get() == 15


def test_pfc_ioc_get_autosave(pfc_ioc):

    autosave_dict: Dict[str, float] = pfc_ioc._get_autosave()

    assert autosave_dict == {
        "pytest_pfcw:FILTER_SET:1:IN:1": 100.0,
        "pytest_pfcw:FILTER_SET:1:IN:2": 100.0,
        "pytest_pfcw:FILTER_SET:2:IN:1": 100.0,
        "pytest_pfcw:FILTER_SET:2:IN:2": 100.0,
        "pytest_pfcw:FILTER_SET:1:OUT:1": 0.0,
        "pytest_pfcw:FILTER_SET:1:OUT:2": 0.0,
        "pytest_pfcw:FILTER_SET:2:OUT:1": 0.0,
        "pytest_pfcw:FILTER_SET:2:OUT:2": 0.0,
        "pytest_pfcw:SHUTTER:OPEN": 0.0,
        "pytest_pfcw:SHUTTER:CLOSED": 500.0,
        "pytest_pfcw:HIGH:THRESHOLD:EXTREME": 10.0,
        "pytest_pfcw:HIGH:THRESHOLD:UPPER": 5.0,
        "pytest_pfcw:HIGH:THRESHOLD:LOWER": 2.0,
        "pytest_pfcw:LOW:THRESHOLD:UPPER": 2.0,
        "pytest_pfcw:LOW:THRESHOLD:LOWER": 5.0,
        "High3": 10000.0,
        "High2": 500.0,
        "High1": 300.0,
        "Low1": 500.0,
        "Low2": 10000.0,
        "pytest_pfcw:FILTER_SET": 1,
    }
