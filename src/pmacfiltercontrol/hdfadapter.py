import asyncio
import codecs
import logging

import json
import h5py
import os

from typing import Callable, Dict, Optional, Union

ATTENUATION_KEY = "attenuation"
ADJUSTMENT_KEY = "adjustment"
FRAME_NUMBER_KEY = "frame_number"
UID_KEY = "/uid"


class HDFAdapter:
    def __init__(
        self,
        hdf_file_path: str = "",
    ) -> None:

        self.file_path: str = hdf_file_path
        self.file: Optional[h5py.File] = None

    def _set_file_path(self, new_file_path: str) -> None:
        
        if self._check_path(new_file_path):
            self.file_path = new_file_path

    def _open_file(self) -> bool:
        if self.file is None:
            if self._check_path():
                self.file = h5py.File(self.file_path, "w", libver="latest")
                print(f"File {self.file} is open.")
        else:
            if self.file_path != self.file.filename:
                print("Another file is already open and being written to.")
                return False
        return True

    def _close_file(self) -> None:
        if self.file is not None:
            try:
                assert isinstance(self.file, h5py.File)
                print(f"File {self.file} has been closed.")
                self.file.close()
                self.file = None
            except Exception as e:
                print(f"Failed closing file.\n{e}")

    def _check_path(self, file_path: str) -> bool:
        if file_path == "" or file_path is None:
            print(
                f"Please enter a valid file path.\nPath={self.file_path}"
            )
        else:
            parent_path: str = file_path.rsplit("/", 1)[0]
            if not os.path.isdir(parent_path):
                print("Path not found. Enter a valid path.")
            else:
                return True

        return False

    def _write_to_file(self, data) -> None:

        assert isinstance(self.file, h5py.File)

        if ADJUSTMENT_KEY not in self.file.keys():
            adjustment_dset = self.file.create_dataset(
                ADJUSTMENT_KEY, (1,), maxshape=(None,), dtype=int
            )
        if ATTENUATION_KEY not in self.file.keys():
            attenuation_dset = self.file.create_dataset(
                ATTENUATION_KEY, (1,), maxshape=(None,), dtype=int
            )

        if UID_KEY not in self.file.keys():
            uid_dataset = self.file.create_dataset(
                UID_KEY, (1,), maxshape=(None,), dtype=int
            )

        adjustment_dset = self.file.get(ADJUSTMENT_KEY)
        assert isinstance(adjustment_dset, h5py.Dataset)
        attenuation_dset = self.file.get(ATTENUATION_KEY)
        assert isinstance(attenuation_dset, h5py.Dataset)
        uid_dataset = self.file.get(UID_KEY)
        assert isinstance(uid_dataset, h5py.Dataset)

        if not self.file.swmr_mode:
            self.file.swmr_mode = True

        assert adjustment_dset.size == attenuation_dset.size
        dset_size = adjustment_dset.size
        if data[FRAME_NUMBER_KEY] >= dset_size:
            assert isinstance(dset_size, np_int64)
            while dset_size <= data[FRAME_NUMBER_KEY]:
                dset_size = dset_size + 1
            adjustment_dset.resize((dset_size,))
            attenuation_dset.resize((dset_size,))
            uid_dataset.resize((dset_size,))

        adjustment_dset[data[FRAME_NUMBER_KEY]] = data[ADJUSTMENT_KEY]
        attenuation_dset[data[FRAME_NUMBER_KEY]] = data[ATTENUATION_KEY]
        uid_dataset[data[FRAME_NUMBER_KEY]] = int(data[FRAME_NUMBER_KEY]) + 1

        adjustment_dset.flush()
        attenuation_dset.flush()
        uid_dataset.flush()
