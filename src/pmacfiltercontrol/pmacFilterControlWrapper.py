import asyncio
import codecs
import logging

import json
import zmq
import h5py
import os
from aioca import caput, caget
from numpy import int64 as np_int64
from datetime import datetime as dt
from softioc import builder
from typing import Callable, Dict, Optional, Union

from .zmqadapter import ZeroMQAdapter


MODE = [
    "MANUAL",
    "CONTINUOUS",
    "SINGLESHOT",
]

FILTER_SET = [
    "Cu",
    "Mo 1",
    "Mo 2",
    "Mo 3",
    "Ag 1",
    "Ag 2",
]

ATTENUATION = [
    0,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
]

ATTENUATION_KEY = "attenuation"
ADJUSTMENT_KEY = "adjustment"
FRAME_NUMBER_KEY = "frame_number"
UID_KEY = "/uid"

SHUTTER_CLOSED = "CLOSED"
SHUTTER_OPEN = "OPEN"


def _if_connected(func: Callable) -> Callable:
    """Decorator function to check if connected to device before calling function.

    Args:
        func (Callable): Function to call if connected to device

    Returns:
        Callable: The function to wrap func in.
    """

    def check_connection(*args, **kwargs) -> Union[Callable, bool]:
        self = args[0]
        if not self.connected:
            print("Not connected to device. Try again once connection resumed.")
            return True
        return func(*args, *kwargs)

    return check_connection


class Wrapper:

    POLL_PERIOD = 0.1
    RETRY_PERIOD = 5

    def __init__(
        self,
        ip: str,
        port: int,
        event_stream_port: int,
        builder: builder,
        device_name: str,
        filter_set_total: int,
        filters_per_set: int,
        detector: str,
        motors: str,
        autosave_pos_file_path: str,
        hdf_file_path: str,
    ):

        self._log = logging.getLogger(self.__class__.__name__)

        self.ip = ip
        self.port = port
        self.zmq_stream = ZeroMQAdapter(ip, port)
        self.event_stream = ZeroMQAdapter(ip, event_stream_port, zmq_type=zmq.SUB)

        self.detector: str = detector
        self.motors: str = motors

        self.autosave_pos_file_path: str = autosave_pos_file_path

        self.status_recv: bool = True
        self.connected: bool = False

        self.h5f: Optional[h5py.File] = None

        self.pixel_count_thresholds = {
            "high1": 2,
            "high2": 2,
            "high3": 100,
            "low1": 2,
            "low2": 2,
        }

        self.device_name = device_name

        self.version = builder.stringIn("VERSION")

        self.state = builder.mbbIn(
            "STATE",
            FFST="TIMEOUT",
            FTST="HIGH3_TRIGGERED",
            ZRST="IDLE",
            ONST="WAITING",
            TWST="ACTIVE",
            THST="SINGLESHOT_WAITING",
            FRST="SINGLESHOT_COMPLETE",
            FFVL=15,
            FTVL=14,
            ZRVL=0,
            ONVL=1,
            TWVL=2,
            THVL=3,
            FRVL=4,
        )

        self.mode = builder.mbbOut(
            "MODE",
            *MODE,
            on_update=self._set_mode,
        )
        self.mode_rbv = builder.mbbIn("MODE_RBV", *MODE)

        self.reset = builder.boolOut("RESET", on_update=self._reset)

        self.timeout = builder.aOut(
            "TIMEOUT", initial_value=3, on_update=self._set_timeout
        )
        self.timeout_rbv = builder.aIn("TIMEOUT_RBV", initial_value=3, EGU="s")

        self.clear_error = builder.boolOut("ERROR:CLEAR", on_update=self._clear_error)

        self.singleshot_start = builder.boolOut(
            "SINGLESHOT:START", on_update=self._start_singleshot
        )

        self.filter_set = builder.mbbOut(
            "FILTER_SET", *FILTER_SET, initial_value=0, on_update=self._set_filter_set
        )
        self.filter_set_rbv = builder.mbbIn(
            "FILTER_SET_RBV", *FILTER_SET, initial_value=0
        )

        self.file_path = builder.longStringOut(
            "FILE:PATH",
            on_update=self._set_file_path,
            FTVL="UCHAR",
            length=256,
            initial_value=f"{hdf_file_path}",
        )
        self.file_path_rbv = builder.longStringIn(
            "FILE:PATH_RBV",
            FTVL="UCHAR",
            length=256,
        )
        self.file_name = builder.longStringOut(
            "FILE:NAME",
            on_update=self._set_file_name,
            FTVL="UCHAR",
            length=256,
            initial_value="attenuation.h5",
        )
        self.file_name_rbv = builder.longStringIn(
            "FILE:NAME_RBV",
            FTVL="UCHAR",
            length=256,
        )
        self.file_full_name = builder.longStringIn(
            "FILE:FULL_NAME",
        )
        self._combine_file_path_and_name()

        self.process_duration = builder.aIn("PROCESS:DURATION", EGU="us")
        self.process_period = builder.aIn("PROCESS:PERIOD", EGU="us")

        self.last_frame_received = builder.aIn("FRAME:RECEIVED")
        self.last_frame_processed = builder.aIn("FRAME:PROCESSED")
        self.time_since_last_frame = builder.aIn("FRAME:LAST_TIME", EGU="s")

        self.current_attenuation = builder.aIn("ATTENUATION_RBV")
        self.attenuation = builder.mbbOut(
            "ATTENUATION", *ATTENUATION, on_update=self._set_manual_attenuation
        )

        self.histogram_scale = builder.aOut(
            "HISTOGRAM:SCALE",
            initial_value=1.0,
            EGU="x",
            on_update=self._set_histogram_scale,
        )

        self.autosave_exists: bool = self._check_autosave_file_exists()
        self._autosave_pos_dict: Dict[str, float] = {}

        if self.autosave_exists:
            self._autosave_pos_dict = self._get_autosave()

        self._generate_filter_pos_records(filter_set_total, filters_per_set)
        self._generate_shutter_records()
        self._generate_pixel_threshold_records()

        self.write_autosave()

    def _check_autosave_file_exists(self) -> bool:
        if os.path.exists(self.autosave_pos_file_path):
            return True
        else:
            return False

    def _get_autosave(self) -> Dict[str, float]:
        pos_dict = {}
        with open(self.autosave_pos_file_path, "r") as pos_file:
            for line in pos_file:
                line = line.strip().split(" ")
                pos_dict[line[0]] = float(line[1])
        return pos_dict

    def write_autosave(self) -> None:

        with open(self.autosave_pos_file_path, "w") as pos_file:
            for key, value in self._autosave_pos_dict.items():
                pos_file.write(f"{key} {value}\n")

        print("Updated autosave file with new positions.")

    def _generate_filter_pos_records(
        self,
        filter_set_total: int,
        filters_per_set: int,
    ) -> None:

        self.filter_sets_in = {}
        self.filter_sets_out = {}
        for i in range(1, filter_set_total + 1):
            filter_set_key = f"filter_set_{i}"
            self.filter_sets_in[filter_set_key] = {}
            self.filter_sets_out[filter_set_key] = {}

            for j in range(1, filters_per_set + 1):

                IN_KEY = f"FILTER_SET:{i}:IN:{j}"
                OUT_KEY = f"FILTER_SET:{i}:OUT:{j}"

                in_value: float = (
                    self._autosave_pos_dict[f"{self.device_name}:{IN_KEY}"]
                    if self.autosave_exists
                    else 100.0
                )
                in_record: builder.aOut = builder.aOut(
                    IN_KEY,
                    initial_value=in_value,
                    on_update=lambda val, i=i: self._set_pos(i, IN_KEY, val),
                )

                out_value: float = (
                    self._autosave_pos_dict[f"{self.device_name}:{OUT_KEY}"]
                    if self.autosave_exists
                    else 0.0
                )
                out_record: builder.aOut = builder.aOut(
                    OUT_KEY,
                    initial_value=out_value,
                    on_update=lambda val, i=i: self._set_pos(i, OUT_KEY, val),
                )

                if not self.autosave_exists:
                    self._autosave_pos_dict[in_record.name] = in_value
                    self._autosave_pos_dict[out_record.name] = out_value

                self.filter_sets_in[filter_set_key][IN_KEY] = in_record
                self.filter_sets_out[filter_set_key][OUT_KEY] = out_record

    def _generate_shutter_records(self) -> None:

        self.shutter = builder.boolOut(
            "SHUTTER:POS", on_update=self._set_shutter, ZNAM="CLOSED", ONAM="OPEN"
        )

        shutter_open_pos = (
            self._autosave_pos_dict[f"{self.device_name}:SHUTTER:OPEN"]
            if self.autosave_exists
            else 0
        )
        shutter_closed_pos = (
            self._autosave_pos_dict[f"{self.device_name}:SHUTTER:CLOSED"]
            if self.autosave_exists
            else 500
        )

        self.shutter_pos_open = builder.aOut(
            "SHUTTER:OPEN",
            initial_value=shutter_open_pos,
            on_update=lambda val: self._set_shutter_pos(val, SHUTTER_OPEN),
        )
        self.shutter_pos_closed = builder.aOut(
            "SHUTTER:CLOSED",
            initial_value=shutter_closed_pos,
            on_update=lambda val: self._set_shutter_pos(val, SHUTTER_CLOSED),
        )

        if not self.autosave_exists:
            self._autosave_pos_dict[f"{self.device_name}:SHUTTER:OPEN"] = 0.0
            self._autosave_pos_dict[f"{self.device_name}:SHUTTER:CLOSED"] = 500.0

    def _generate_pixel_threshold_records(self) -> None:

        self.extreme_high_threshold = builder.aOut(
            "HIGH:THRESHOLD:EXTREME",
            initial_value=100,
            on_update=self._set_extreme_high_threshold,
        )
        self.upper_high_threshold = builder.aOut(
            "HIGH:THRESHOLD:UPPER",
            initial_value=2,
            on_update=self._set_upper_high_threshold,
        )
        self.lower_high_threshold = builder.aOut(
            "HIGH:THRESHOLD:LOWER",
            initial_value=2,
            on_update=self._set_lower_high_threshold,
        )
        self.upper_low_threshold = builder.aOut(
            "LOW:THRESHOLD:UPPER",
            initial_value=2,
            on_update=self._set_upper_low_threshold,
        )
        self.lower_low_threshold = builder.aOut(
            "LOW:THRESHOLD:LOWER",
            initial_value=2,
            on_update=self._set_lower_low_threshold,
        )

        pixel_threshold_records = [
            self.extreme_high_threshold,
            self.upper_high_threshold,
            self.lower_high_threshold,
            self.upper_low_threshold,
            self.lower_low_threshold,
        ]

        for record in pixel_threshold_records:
            if not self.autosave_exists:
                self._autosave_pos_dict[record.name] = record.get()
            else:
                record.set(self._autosave_pos_dict[record.name])

    async def _get_initial_hists(self) -> None:

        self._initial_hists: Dict[str, float] = {
            "High3": await caget(f"{self.detector}:OD:SUM:Histogram:High3"),
            "High2": await caget(f"{self.detector}:OD:SUM:Histogram:High2"),
            "High1": await caget(f"{self.detector}:OD:SUM:Histogram:High1"),
            "Low1": await caget(f"{self.detector}:OD:SUM:Histogram:Low1"),
            "Low2": await caget(f"{self.detector}:OD:SUM:Histogram:Low2"),
        }

    async def run_forever(self) -> None:

        print("Connecting to ZMQ stream...")

        await asyncio.gather(
            *[
                self.monitor_command_stream(self.zmq_stream),
                self.monitor_event_stream(self.event_stream),
                self.zmq_stream.run_forever(),
                self.event_stream.run_forever(),
                self._query_status(),
            ]
        )

    async def monitor_command_stream(self, zmq_stream: ZeroMQAdapter) -> None:
        while True:
            if not self.zmq_stream.running:
                await asyncio.sleep(1)
            else:
                resp: bytes = await zmq_stream.get_response()
                resp_json = json.loads(resp)

                if "status" in resp_json:
                    if not self.connected:
                        self.connected = True
                    status = resp_json["status"]
                    self._handle_status(status)
                    self.status_recv = True

    async def monitor_event_stream(self, zmq_stream: ZeroMQAdapter) -> None:
        while True:
            if not self.zmq_stream.running:
                await asyncio.sleep(1)
            else:
                resp: bytes = await zmq_stream.get_response()
                resp_json = json.loads(resp)

                if "frame_number" in resp_json:
                    file_open = self._open_file()
                    if file_open:
                        await self._write_to_hdf5(resp_json)

    def _open_file(self) -> bool:
        if self.h5f is None:
            if self._check_path():
                self.h5f = h5py.File(self.file_full_name.get(), "w", libver="latest")
                print(f"File {self.h5f} is open.")
        else:
            if self.file_full_name.get() != self.h5f.filename:
                print("Another file is already open and being written to.")
                return False
        return True

    def _close_file(self) -> None:
        assert isinstance(self.h5f, h5py.File)
        print(f"File {self.h5f} has been closed.")
        self.h5f.close()
        self.h5f = None

    def _req_status(self) -> None:
        req_status = b'{"command":"status"}'
        self._send_message(req_status)
    async def _query_status(self) -> None:
        while True:
            if not self.zmq_stream.running:
                print("Zmq stream not running. waiting...")
                await asyncio.sleep(1)
            else:
                if self.status_recv:
                    self.status_recv = False
                    self._req_status()
                else:
                    print("No status response. Waiting for reconnect...")
                    self.connected = False
                    while not self.status_recv:
                        await asyncio.sleep(1)
                        self._req_status()
                    print("Reconnected and status recieved.")
                await asyncio.sleep(0.1)

    def _handle_status(self, status) -> None:

        state = status["state"]
        if state < 0:
            state = 16 + state
        self.state.set(state)

        version = status["version"]
        self.version.set(str(version))

        process_duration = status["process_duration"]
        self.process_duration.set(process_duration)

        process_period = status["process_period"]
        self.process_period.set(process_period)

        last_received_frame = status["last_received_frame"]
        self.last_frame_received.set(last_received_frame)

        last_processed_frame = status["last_processed_frame"]
        self.last_frame_processed.set(last_processed_frame)

        time_since_last_frame = status["time_since_last_message"]
        self.time_since_last_frame.set(time_since_last_frame)
        if time_since_last_frame > self.timeout_rbv.get() and self.h5f is not None:
            try:
                self._close_file()
            except Exception as e:
                print(f"Failed closing file.\n{e}")

        current_attenuation = status["current_attenuation"]
        self.current_attenuation.set(current_attenuation)

    def _send_message(self, message: bytes) -> None:
        self.zmq_stream.send_message([message])

    def _configure_param(self, param: Dict[str, Union[int, Dict[str, int]]]) -> None:
        configure = json.dumps({"command": "configure", "params": param})
        self._send_message(codecs.encode(configure, "utf-8"))

    @_if_connected
    def _set_mode(self, mode: int) -> None:

        # Set mode for PFC
        self._configure_param({"mode": mode})

        self.mode_rbv.set(mode)

    @_if_connected
    def _set_manual_attenuation(self, attenuation: int) -> None:

        if self.state.get() == 0 and self.mode_rbv.get() == 0:
            # Set manual attenuation for PFC
            self._configure_param({"attenuation": attenuation})

        else:
            print("ERROR: Must be in MANUAL mode and IDLE state.")

    @_if_connected
    async def _reset(self, _) -> None:
        if _ == 1:
            reset = b'{"command":"reset"}'
            self._send_message(reset)

            self.reset.set(0)

    @_if_connected
    def _set_timeout(self, timeout: int) -> None:

        # Set timeout for PFC
        self._configure_param({"timeout": timeout})

        self.timeout_rbv.set(timeout)

    @_if_connected
    async def _clear_error(self, _) -> None:

        if _ == 1:
            clear_error = json.dumps({"command": "clear_error"})
            self._send_message(codecs.encode(clear_error, "utf-8"))

            self.clear_error.set(0)

    @_if_connected
    async def _start_singleshot(self, _) -> None:

        if _ == 1:

            if (
                self.state.get() == 3 or self.state.get() == 4
            ) and self.mode_rbv.get() == 2:
                start_singleshot = json.dumps({"command": "singleshot"})
                self._send_message(codecs.encode(start_singleshot, "utf-8"))
            else:
                print("ERROR: Must be in SINGLESHOT mode and WAITING state.")

            self.singleshot_start.set(0)

    @_if_connected
    async def _set_shutter(self, shutter_state: int) -> None:
        if shutter_state == SHUTTER_CLOSED:
            pos = self.shutter_pos_closed.get()
        else:
            pos = self.shutter_pos_open.get()
        await caput(f"{self.motors}:SHUTTER:POS", pos)

    def _set_shutter_pos(self, val: float, shutter_state: int) -> None:

        if shutter_state == SHUTTER_CLOSED:
            self._configure_param({"shutter_closed_position": val})

        current_shutter_state = "CLOSED" if self.shutter.get() == 0 else "OPEN"
        if current_shutter_state == shutter_state:
            self._set_shutter(shutter_state)

        self._autosave_pos_dict[f"{self.device_name}:SHUTTER:{shutter_state}"] = val

        self.write_autosave()

    @_if_connected
    def _set_thresholds(self) -> None:

        self._configure_param({"pixel_count_thresholds": self.pixel_count_thresholds})

    @_if_connected
    def _set_extreme_high_threshold(self, threshold: int) -> None:

        if threshold != self.pixel_count_thresholds["high3"]:
            self.pixel_count_thresholds["high3"] = threshold

            # Set upper high threshold for PFC
            self._set_thresholds()

    @_if_connected
    def _set_upper_high_threshold(self, threshold: int) -> None:

        if threshold != self.pixel_count_thresholds["high2"]:
            self.pixel_count_thresholds["high2"] = threshold

            # Set upper high threshold for PFC
            self._set_thresholds()

        else:
            print(f"High 2 is already at value {threshold}.")

    @_if_connected
    def _set_lower_high_threshold(self, threshold: int) -> None:

        if threshold != self.pixel_count_thresholds["high1"]:
            self.pixel_count_thresholds["high1"] = threshold

            # Set lower high threshold for PFC
            self._set_thresholds()

        else:
            print(f"High 1 is already at value {threshold}.")

    @_if_connected
    def _set_upper_low_threshold(self, threshold: int) -> None:

        if threshold != self.pixel_count_thresholds["low2"]:
            self.pixel_count_thresholds["low2"] = threshold

            # Set upper high threshold for PFC
            self._set_thresholds()

        else:
            print(f"Low 2 is already at value {threshold}.")

    @_if_connected
    def _set_lower_low_threshold(self, threshold: int) -> None:

        if threshold != self.pixel_count_thresholds["low1"]:
            self.pixel_count_thresholds["low1"] = threshold

            # Set upper high threshold for PFC
            self._set_thresholds()

        else:
            print(f"Low 1 is already at value {threshold}.")

    @_if_connected
    async def _set_histogram_scale(self, scale: float) -> None:

        new_thresholds = self._initial_hists

        if scale != 1.0:
            await self._get_initial_hists()

            new_thresholds = {
                key: threshold * scale for key, threshold in self._initial_hists.items()
            }

        for key, threshold in new_thresholds.items():
            caput(f"{self.detector}:OD:SUM:Histogram:{key}", threshold)

    @_if_connected
    def _set_filter_set(self, filter_set_num: int) -> None:

        in_positions = [
            x.get()
            for x in self.filter_sets_in[f"filter_set_{filter_set_num+1}"].values()
        ]
        out_positions = [
            x.get()
            for x in self.filter_sets_out[f"filter_set_{filter_set_num+1}"].values()
        ]

        in_pos = {}
        for id, pos in enumerate(in_positions):
            in_pos[f"filter{id+1}"] = pos

        out_pos = {}
        for id, pos in enumerate(out_positions):
            out_pos[f"filter{id+1}"] = pos

        # Set filter set positions for PFC
        self._configure_param({"in_positions": in_pos, "out_positions": out_pos})

        self.filter_set_rbv.set(filter_set_num)

    @_if_connected
    def _set_pos(self, filter_set: int, in_out_key: str, val: float) -> None:

        self._autosave_pos_dict[f"{self.device_name}:{in_out_key}"] = val

        if self.filter_set_rbv.get() == filter_set - 1:
            self._set_filter_set(filter_set - 1)

        self.write_autosave()

    @_if_connected
    def _set_file_path(self, path: str) -> None:

        # Set file path for PFC
        self.file_path_rbv.set(path)

        self._combine_file_path_and_name()

    @_if_connected
    def _set_file_name(self, name: str) -> None:

        # Set file name for PFC
        self.file_name_rbv.set(name)

        self._combine_file_path_and_name()

    def _combine_file_path_and_name(self, exists: bool = False) -> None:

        path: str = self.file_path.get()
        if exists:
            name = "".join(
                [self.file_name.get().strip(".h5"), f"-{dt.time(dt.now())}.h5"]
            )
        else:
            name = self.file_name.get()

        full_path: str = "/".join([path, name])

        self.file_full_name.set(full_path)

    def _check_path(self) -> bool:
        if self.file_path.get() == "" or self.file_name.get() == "":
            print(
                f"Please enter a valid file path and name.\nPath={self.file_path.get()}\nName={self.file_name.get()}\nFullPath={self.file_full_name.get()}"
            )
        elif not os.path.isdir(self.file_path.get()):
            parent_path: str = self.file_path.get().rsplit("/", 1)[0]
            dir_name: str = self.file_path.get().rsplit("/", 1)[1]
            if not os.path.isdir(parent_path):
                print("Path not found. Enter a valid path.")
            else:
                print("Parent path exists, making new dir")
                os.makedirs(dir_name, exist_ok=True)
                return True
        elif os.path.isdir(self.file_path.get()):
            if os.path.isfile(self.file_full_name.get()):
                self._combine_file_path_and_name(exists=True)
            return True

        return False

    async def _write_to_hdf5(self, data) -> None:

        assert isinstance(self.h5f, h5py.File)

        if ADJUSTMENT_KEY not in self.h5f.keys():
            adjustment_dset = self.h5f.create_dataset(
                ADJUSTMENT_KEY, (128,), maxshape=(None,), dtype=int
            )
        if ATTENUATION_KEY not in self.h5f.keys():
            attenuation_dset = self.h5f.create_dataset(
                ATTENUATION_KEY, (128,), maxshape=(None,), dtype=int
            )

        if UID_KEY not in self.h5f.keys():
            uid_dataset = self.h5f.create_dataset(
                UID_KEY, (128,), maxshape=(None,), dtype=int
            )

        adjustment_dset = self.h5f.get(ADJUSTMENT_KEY)
        assert isinstance(adjustment_dset, h5py.Dataset)
        attenuation_dset = self.h5f.get(ATTENUATION_KEY)
        assert isinstance(attenuation_dset, h5py.Dataset)
        uid_dataset = self.h5f.get(UID_KEY)
        assert isinstance(uid_dataset, h5py.Dataset)

        self.h5f.swmr_mode = True

        assert adjustment_dset.size == attenuation_dset.size
        dset_size = adjustment_dset.size
        if data[FRAME_NUMBER_KEY] >= dset_size:
            assert isinstance(dset_size, np_int64)
            while dset_size <= data[FRAME_NUMBER_KEY]:
                dset_size = dset_size + 1
            adjustment_dset.resize((dset_size,))
            attenuation_dset.resize((dset_size,))
            uid_dataset.rezise((dset_size,))

        adjustment_dset[data[FRAME_NUMBER_KEY]] = data[ADJUSTMENT_KEY]
        attenuation_dset[data[FRAME_NUMBER_KEY]] = data[ATTENUATION_KEY]
        uid_dataset[data[FRAME_NUMBER_KEY]] = int(data[FRAME_NUMBER_KEY]) + 1
