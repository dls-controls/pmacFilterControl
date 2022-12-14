import h5py
import os
from numpy import int64 as np_int64

from typing import Optional

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

    def _open_file(self) -> None:
        if self.file is None:
            if self._check_path():
                self.file = h5py.File(self.file_path, "w", libver="latest")
                print(f"* File {self.file} is open.")
                self._setup_datasets()
        else:
            if self.file_path != self.file.filename:
                print("* Another file is already open and being written to.")

    def _close_file(self) -> None:
        if self.file is not None:
            try:
                assert isinstance(self.file, h5py.File)
                print(f"* File {self.file} has been closed.")
                self.file.close()
                self.file = None
            except Exception as e:
                print(f"* Failed closing file.\n{e}")
        else:
            print(f"* No file is open, ignoring close...")

    def _check_path(self, file_path: str) -> bool:
        if file_path == "" or file_path is None:
            print(f"* Please enter a valid file path.\nPath={self.file_path}")
        else:
            parent_path: str = file_path.rsplit("/", 1)[0]
            if not os.path.isdir(parent_path):
                print("* Path not found. Enter a valid path.")
            else:
                return True

        return False

    def _setup_datasets(self) -> None:

        print(f"* Creating/fetching datasets in HDF5 file: {self.file_path}")

        def _fetch_dataset(key: str) -> h5py.Dataset:
            dset: h5py.Dataset = None
            if key not in self.file.keys():
                dset = self.file.create_dataset(key, (1,), maxshape=(None,), dtype=int)
            else:
                dset = self.file.get(key)
            return dset

        self.adjustment_dset = _fetch_dataset(ADJUSTMENT_KEY)
        self.attenuation_dset = _fetch_dataset(ATTENUATION_KEY)
        self.uid_dataset = _fetch_dataset(UID_KEY)

    def _write_to_file(self, data) -> None:

        if not self.file.swmr_mode:
            self.file.swmr_mode = True

        assert self.adjustment_dset.size == self.attenuation_dset.size
        dset_size = self.adjustment_dset.size
        if data[FRAME_NUMBER_KEY] >= dset_size:
            assert isinstance(dset_size, np_int64)
            while dset_size <= data[FRAME_NUMBER_KEY]:
                dset_size = dset_size + 1
            self.adjustment_dset.resize((dset_size,))
            self.attenuation_dset.resize((dset_size,))
            self.uid_dataset.resize((dset_size,))

        self.adjustment_dset[data[FRAME_NUMBER_KEY]] = data[ADJUSTMENT_KEY]
        self.attenuation_dset[data[FRAME_NUMBER_KEY]] = data[ATTENUATION_KEY]
        self.uid_dataset[data[FRAME_NUMBER_KEY]] = int(data[FRAME_NUMBER_KEY]) + 1

        self.adjustment_dset.flush()
        self.attenuation_dset.flush()
        self.uid_dataset.flush()
