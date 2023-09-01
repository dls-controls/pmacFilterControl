from pathlib import Path
from typing import Dict, Optional, Set

import h5py
import pytest
from mock import MagicMock, Mock
from numpy import int64

from pmacfiltercontrol.hdfadapter import HDFAdapter

# -------------------------------------------------
# H5Py


@pytest.fixture
def mock_hdf5_file() -> MagicMock:
    file = MagicMock(spec=h5py.File)
    file.__enter__.return_value = {"test": 1}
    return file


@pytest.fixture
def mock_h5py_file(mocker, mock_hdf5_file):
    # def _add_to_dict(*args):
    #     mock_hdf5_file.file.__enter__.append()

    mocker.patch("h5py.File", return_value=mock_hdf5_file)
    return mocker


@pytest.fixture
def mock_dataset() -> MagicMock:
    dataset = MagicMock(spec=h5py.Dataset)

    dataset.size = int64(1)

    def resize(inc: Set[Optional[int]]):
        dataset.size += inc

    dataset.resize = Mock(side_effect=resize)
    dataset.flush = Mock()

    return dataset


# -------------------------------------------------
# HDF Adapter


@pytest.fixture
def hdf_adapter() -> HDFAdapter:
    return HDFAdapter()


# -------------------------------------------------
# Tests


def test_hdf_adapter_constructor():
    HDFAdapter()


def test_hdf_adapter_set_file_path(hdf_adapter: HDFAdapter):
    hdf_adapter._check_path = Mock()
    hdf_adapter._check_path.return_value = True

    hdf_adapter._set_file_path("./test_hdf5.h5")

    hdf_adapter._check_path.assert_called_once()
    assert hdf_adapter.file_path == "./test_hdf5.h5"


def test_hdf_adapter_open_file(hdf_adapter: HDFAdapter, mock_h5py_file):
    hdf_adapter._check_path = Mock()
    hdf_adapter._check_path.return_value = True
    hdf_adapter._setup_datasets = Mock()

    assert hdf_adapter.file_open is False

    hdf_adapter._open_file()

    assert hdf_adapter.file_open is True


def test_hdf_adapter_open_file_already_open(
    hdf_adapter: HDFAdapter, mock_hdf5_file, capsys
):
    hdf_adapter.file = mock_hdf5_file
    hdf_adapter.file_path = "test_path"

    hdf_adapter._open_file()

    captured = capsys.readouterr()
    assert captured.out == "* Another file is already open and being written to.\n"


def test_hdf_adapter_close_file(hdf_adapter: HDFAdapter, mock_hdf5_file, capsys):
    hdf_adapter.file = mock_hdf5_file
    hdf_adapter.file_open = True

    assert hdf_adapter.file_open is True

    hdf_adapter._close_file()

    captured = capsys.readouterr()
    assert captured.out == f"* File {mock_hdf5_file} has been closed.\n"

    assert hdf_adapter.file is None
    assert hdf_adapter.file_open is False


def test_hdf_adapter_close_file_exception(
    hdf_adapter: HDFAdapter, mock_hdf5_file, capsys
):
    hdf_adapter.file = "test_string_to_cause_exception"

    hdf_adapter._close_file()

    captured = capsys.readouterr()
    assert captured.out == "* Failed closing file.\n\n"


def test_hdf_adapter_check_path_is_none(hdf_adapter: HDFAdapter, capsys):
    ret = hdf_adapter._check_path(file_path=None)

    captured = capsys.readouterr()
    assert captured.out == "* Please enter a valid file path.\nPath=\n"

    assert ret is False


def test_hdf_adapter_check_path_not_found(hdf_adapter: HDFAdapter, capsys):
    ret = hdf_adapter._check_path(file_path="test_path")

    captured = capsys.readouterr()
    assert captured.out == "* Path not found. Enter a valid path.\n"

    assert ret is False


def test_hdf_adapter_check_path_exists(hdf_adapter: HDFAdapter, capsys):
    ret = hdf_adapter._check_path(file_path=str(Path(__file__)))

    captured = capsys.readouterr()
    assert captured.out == "* File already exists.\n"

    assert ret is False


def test_hdf_adapter_check_path(hdf_adapter: HDFAdapter):
    ret = hdf_adapter._check_path(file_path="./test_file")

    assert ret is True


def test_hdf_adapter_setup_datasets(hdf_adapter: HDFAdapter, mock_hdf5_file):
    def return_dataset(key: str, *args, **kwargs) -> Dict[str, str]:
        return {"test_dataset": key}

    hdf_adapter.file = mock_hdf5_file
    hdf_adapter.file.swmr_mode is False

    mock_hdf5_file.create_dataset = Mock(side_effect=return_dataset)

    hdf_adapter._setup_datasets()

    assert hdf_adapter.adjustment_dset == {"test_dataset": "adjustment"}
    assert hdf_adapter.attenuation_dset == {"test_dataset": "attenuation"}
    assert hdf_adapter.uid_dataset == {"test_dataset": "uid"}
    assert hdf_adapter.filters_moving_flag_dataset == {"test_dataset": "filters_moving"}

    assert hdf_adapter.file.swmr_mode is True


def test_hdf_adapter_write_to_file(hdf_adapter: HDFAdapter, mock_dataset):
    hdf_adapter.adjustment_dset = mock_dataset
    hdf_adapter.attenuation_dset = mock_dataset
    hdf_adapter.uid_dataset = mock_dataset
    hdf_adapter.filters_moving_flag_dataset = mock_dataset

    data = {
        "frame_number": 1,
        "adjustment": 1,
        "attenuation": 1,
        "uid": 1,
        "filters_moving": 1,
    }

    hdf_adapter._write_to_file(data=data)

    assert mock_dataset.resize.call_count == 4
    assert mock_dataset.__setitem__.call_count == 4
    assert mock_dataset.flush.call_count == 4
