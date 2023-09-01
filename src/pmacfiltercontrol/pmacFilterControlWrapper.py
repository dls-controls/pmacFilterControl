"""The PMAC Filter Control wrapper."""

import asyncio
import codecs
import json
from datetime import datetime as dt
from pathlib import Path
from typing import Callable, Dict, Union

import zmq
from aioca import caget, caput
from softioc import builder
from softioc.builder import records

from .hdfadapter import HDFAdapter
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

SHUTTER_CLOSED = 0
SHUTTER_OPEN = 1


def _if_connected(func: Callable) -> Callable:
    """
    Check connection decorator before function call.

    Decorator function to check if the wrapper is connected to the motion controller
    device before calling the attached function.

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
    """Wrapper object for PMAC Filter Control.

    The wrapper object for PMAC Filter Control, which initialises all of the CA records
    with relevant python functions attached and initialises the asyncio logic loops.
    """

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
        autosave_file_path: str,
        hdf_file_path: str,
    ):
        """
        PMAC Filter Control wrapper constuctor.

        Args:
            ip (str): IP address of PowerBrick
            port (int): Port of ZeroMQ command stream
            event_stream_port (int): Port of ZeroMQ event stream
            builder (builder): SoftIOC builder object
            device_name (str): Name of the PFC device
            filter_set_total (int): Number of filter sets
            filters_per_set (int): Number of filters per filter set
            detector (str): Detector PV prefix
            motors (str): Motor IOC PV prefix
            autosave_file_path (str): Autosave file location and name
            hdf_file_path (str): HDF5 file path for attenuation data
        """
        self.ip = ip
        self.port = port
        self.zmq_stream = ZeroMQAdapter(ip, port)
        self.event_stream = ZeroMQAdapter(ip, event_stream_port, zmq_type=zmq.SUB)

        self.detector: str = detector
        self.motors: str = motors

        self.autosave_file: Path = Path(autosave_file_path)

        self.status_recv: bool = True
        self.connected: bool = False

        self.h5f: HDFAdapter = HDFAdapter(hdf_file_path)

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
        self.state.add_metadata("archiver 1 Monitor")

        self.mode = builder.mbbOut(
            "MODE",
            *MODE,
            on_update=self._set_mode,
        )
        self.mode_rbv = builder.mbbIn("MODE_RBV", *MODE)

        self.reset = builder.boolOut("RESET", on_update=self._reset)
        self._reset_reset = records.calcout(
            "RESET:RESET",
            CALC="A ? 0 : 1",
            INPA=builder.CP(self.reset),
            OUT=builder.PP(self.reset),
            OOPT="When Zero",
        )

        self.timeout = builder.aOut(
            "TIMEOUT", initial_value=3, on_update=self._set_timeout
        )
        self.timeout_rbv = builder.aIn("TIMEOUT_RBV", initial_value=3, EGU="s")

        self.clear_error = builder.boolOut("ERROR:CLEAR", on_update=self._clear_error)
        self._reset_error = records.calcout(
            "ERROR:RESET",
            CALC="A ? 0 : 1",
            INPA=builder.CP(self.clear_error),
            OUT=builder.PP(self.clear_error),
            OOPT="When Zero",
        )

        self.singleshot_start = builder.boolOut(
            "SINGLESHOT:START", on_update=self._start_singleshot
        )
        self._reset_singelshot = records.calcout(
            "SINGLESHOT:RESET",
            CALC="A ? 0 : 1",
            INPA=builder.CP(self.singleshot_start),
            OUT=builder.PP(self.singleshot_start),
            OOPT="When Zero",
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

        self.file_open = builder.aOut(
            "FILE:OPEN",
            on_update=self.open_file,
        )
        self.file_close = builder.aOut(
            "FILE:CLOSE",
            on_update=self.close_file,
        )

        self.process_duration = builder.aIn("PROCESS:DURATION", EGU="us")
        self.process_period = builder.aIn("PROCESS:PERIOD", EGU="us")

        self.last_frame_received = builder.aIn("FRAME:RECEIVED")
        self.last_frame_processed = builder.aIn("FRAME:PROCESSED")
        self.time_since_last_frame = builder.aIn("FRAME:LAST_TIME", EGU="s")

        self.current_attenuation = builder.aIn("ATTENUATION_RBV")
        self.attenuation = builder.mbbOut(
            "ATTENUATION", *ATTENUATION, on_update=self._set_manual_attenuation
        )

        self._hist_thresholds: Dict[str, builder.aOut] = {}
        for threshold in ("High3", "High2", "High1", "Low2", "Low1"):
            hist = builder.aOut(
                f"HIST:{threshold.upper()}",
                on_update=lambda val, threshold=threshold: self._set_hist(
                    threshold, val
                ),
            )
            self._hist_thresholds[threshold] = hist

        self.histogram_scale = builder.aOut(
            "HISTOGRAM:SCALE",
            initial_value=1.0,
            EGU="x",
            on_update=self._set_histogram_scale,
        )

        self._autosave_dict: Dict[str, float] = {}

        if self.autosave_file.exists():
            print("--- Autosave exists, restoring ---")
            self._autosave_dict = self._get_autosave()

        self._generate_filter_pos_records(filter_set_total, filters_per_set)
        self._generate_shutter_records()
        self._generate_pixel_threshold_records()

    async def _send_initial_config(self) -> None:
        """
        Send initial configuration settings.

        Send initial configuration settings on startup once a connection is
        established to the PowerBrick program.
        """
        print("~ Initial Config: Waiting For Connection")
        while not self.connected:
            await asyncio.sleep(0.5)

        self._configure_param(
            {"shutter_closed_position": self.shutter_pos_closed.get()}
        )

        if f"{self.device_name}:FILTER_SET" in self._autosave_dict.keys():
            autosaved_filter_set: int = int(
                self._autosave_dict[f"{self.device_name}:FILTER_SET"]
            )
            print(f"~ Restoring with filter set: {FILTER_SET[autosaved_filter_set]}")
            self.filter_set.set(autosaved_filter_set, process=False)
            self._set_filter_set(autosaved_filter_set)
        else:
            self._set_filter_set(0)

        self.attenuation.set(15)
        asyncio.run_coroutine_threadsafe(
            self._setup_hist_thresholds(), asyncio.get_event_loop()
        )
        print("~ Initial Config: Complete")

    def _get_autosave(self) -> Dict[str, float]:
        """Read the autosave file.

        Opens the autosave file and reads the saved values into a dictionary.

        Returns:
            Dict[str, float]: Dictionary of the values from the autosave file.
        """
        autosave_dict = {}
        with self.autosave_file.open("r") as autosave_file:
            for line in autosave_file:
                line_ = line.strip().split(" ")
                autosave_dict[line_[0]] = float(line_[1])
        return autosave_dict

    def write_autosave(self) -> None:
        """
        Write to autosave files.

        Write the current autosave dictionary to the autosave files, with a
        ' ' delimiter between key/value and each separated by a newline.
        """
        parent_dir = self.autosave_file.parent
        self.autosave_datetime: dt = dt.now()
        self.autosave_backup_file: Path = parent_dir.joinpath(
            f"autosave-{self.autosave_datetime:%Y%m%d-%H}.txt"
        )

        for autosave_file in [self.autosave_file, self.autosave_backup_file]:
            autosave_file.write_text(
                "\n".join(
                    f"{key} {value}" for key, value in self._autosave_dict.items()
                )
            )

            print(f"Updated {autosave_file.name} with new positions.")

    def _generate_filter_pos_records(
        self,
        filter_set_total: int,
        filters_per_set: int,
    ) -> None:
        """
        Generate filter position records.

        Generate the filter in/out position records for each filter set, based on the
        values provided by filter_set_total and filters_per_set.

        Args:
            filter_set_total (int): The number of filter sets
            filters_per_set (int): The number of filters per filter set
        """
        self.filter_sets_in: Dict[str, Dict[str, builder.aOut]] = {}
        self.filter_sets_out: Dict[str, Dict[str, builder.aOut]] = {}

        for i in range(1, filter_set_total + 1):
            filter_set_key = f"filter_set_{i}"
            self.filter_sets_in[filter_set_key] = {}
            self.filter_sets_out[filter_set_key] = {}

            for j in range(1, filters_per_set + 1):
                IN_KEY = f"FILTER_SET:{i}:IN:{j}"
                OUT_KEY = f"FILTER_SET:{i}:OUT:{j}"

                in_value: float = (
                    self._autosave_dict[f"{self.device_name}:{IN_KEY}"]
                    if self.autosave_file.exists()
                    else 100.0
                )
                in_record: builder.aOut = builder.aOut(
                    IN_KEY,
                    initial_value=in_value,
                    on_update=lambda val, i=i, in_key=IN_KEY: self._set_pos(
                        i, in_key, val
                    ),
                )

                out_value: float = (
                    self._autosave_dict[f"{self.device_name}:{OUT_KEY}"]
                    if self.autosave_file.exists()
                    else 0.0
                )
                out_record: builder.aOut = builder.aOut(
                    OUT_KEY,
                    initial_value=out_value,
                    on_update=lambda val, i=i, out_key=OUT_KEY: self._set_pos(
                        i, out_key, val
                    ),
                )

                if not self.autosave_file.exists():
                    self._autosave_dict[in_record.name] = in_value
                    self._autosave_dict[out_record.name] = out_value

                self.filter_sets_in[filter_set_key][IN_KEY] = in_record
                self.filter_sets_out[filter_set_key][OUT_KEY] = out_record

    def _generate_shutter_records(self) -> None:
        """
        Generate fast shutter records.

        Generate records associated with the fast shutter positions.
        """
        self.shutter = builder.boolOut(
            "SHUTTER:POS", on_update=self._set_shutter, ZNAM="CLOSED", ONAM="OPEN"
        )

        shutter_open_pos = (
            self._autosave_dict[f"{self.device_name}:SHUTTER:OPEN"]
            if self.autosave_file.exists()
            else 0
        )
        shutter_closed_pos = (
            self._autosave_dict[f"{self.device_name}:SHUTTER:CLOSED"]
            if self.autosave_file.exists()
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

        if not self.autosave_file.exists():
            self._autosave_dict[f"{self.device_name}:SHUTTER:OPEN"] = 0.0
            self._autosave_dict[f"{self.device_name}:SHUTTER:CLOSED"] = 500.0

    def _generate_pixel_threshold_records(self) -> None:
        """
        Generate pixel threshold records.

        Generate records associated with the pixel threshold values.
        """
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
            if not self.autosave_file.exists():
                self._autosave_dict[record.name] = record.get()
            else:
                record.set(self._autosave_dict[record.name])

    async def _setup_hist_thresholds(self) -> None:
        """
        Histogram threshold value setup.

        Setup the values for the histogram thresholds based on Odin records if no
        autosave exists.
        """
        self._hist_threshold_values: Dict[str, float] = {
            "High3": self._autosave_dict["High3"]
            if "High3" in self._autosave_dict.keys()
            else await caget(f"{self.detector}:OD:SUM:Histogram:High3"),
            "High2": int(self._autosave_dict["High2"])
            if "High2" in self._autosave_dict.keys()
            else await caget(f"{self.detector}:OD:SUM:Histogram:High2"),
            "High1": int(self._autosave_dict["High1"])
            if "High1" in self._autosave_dict.keys()
            else await caget(f"{self.detector}:OD:SUM:Histogram:High1"),
            "Low1": int(self._autosave_dict["Low1"])
            if "Low1" in self._autosave_dict.keys()
            else await caget(f"{self.detector}:OD:SUM:Histogram:Low1"),
            "Low2": int(self._autosave_dict["Low2"])
            if "Low2" in self._autosave_dict.keys()
            else await caget(f"{self.detector}:OD:SUM:Histogram:Low2"),
        }

        for key, value in self._hist_threshold_values.items():
            self._autosave_dict[key] = value
            self._hist_thresholds[key].set(value, process=True)

    async def _get_hist_thresholds(self) -> None:
        """
        Histogram threshold value fetch.

        Fetch the current histogram threshold values from Odin.
        """
        non_scaled_hist_thresholds: Dict[str, float] = {
            "High3": await caget(f"{self.detector}:OD:SUM:Histogram:High3"),
            "High2": await caget(f"{self.detector}:OD:SUM:Histogram:High2"),
            "High1": await caget(f"{self.detector}:OD:SUM:Histogram:High1"),
            "Low1": await caget(f"{self.detector}:OD:SUM:Histogram:Low1"),
            "Low2": await caget(f"{self.detector}:OD:SUM:Histogram:Low2"),
        }

        for key, value in non_scaled_hist_thresholds.items():
            self._autosave_dict[key] = value

    async def _set_hist_thresholds(self, thresholds: Dict[str, int]) -> None:
        """
        Set histogram thresholds.

        Args:
            thresholds (Dict[str, int]): Dictionary of histogram thresholds to set
        """
        for threshold, value in thresholds.items():
            await caput(
                f"{self.detector}:OD:SUM:Histogram:{threshold}",
                value,
            )

        self.write_autosave()

    async def run_forever(self) -> None:
        """Run asyncio background tasks until program exit."""
        print("Connecting to ZMQ stream...")

        asyncio.run_coroutine_threadsafe(
            self._send_initial_config(), asyncio.get_running_loop()
        )

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
        """
        Command stream monitor loop.

        Loop to forever monitor the command stream for responses.

        Args:
            zmq_stream (ZeroMQAdapter): Command stream ZeroMQ object
        """
        while True:
            if not zmq_stream.running:
                print("- Command stream disconnected. Waiting for reconnect...")
                while not zmq_stream.running:
                    await asyncio.sleep(1)
                print("- Command stream (re)connected.")
            else:
                resp: bytes = await zmq_stream.get_response()
                if resp is not None:
                    resp_json = json.loads(resp)

                    if "status" in resp_json:
                        if not self.connected:
                            self.connected = True
                        status = resp_json["status"]
                        self._handle_status(status)
                        self.status_recv = True

    async def monitor_event_stream(self, zmq_stream: ZeroMQAdapter) -> None:
        """
        Event stream monitor loop.

        Loop to forever monitor the event stream for responses.

        Args:
            zmq_stream (ZeroMQAdapter): Event stream ZeroMQ object
        """
        while True:
            if not zmq_stream.running:
                print("- Event stream disconnected. Waiting for reconnect...")
                while not zmq_stream.running:
                    await asyncio.sleep(1)
                print("- Event stream (re)connected.")
            else:
                resp: bytes = await zmq_stream.get_response()
                if resp is not None:
                    resp_json = json.loads(resp)

                    if "frame_number" in resp_json:
                        if self.h5f.file_open:
                            try:
                                self.h5f._write_to_file(resp_json)
                            except RuntimeError as e:
                                print(e)
                        else:
                            print("WARNING: HDF5 file not open and frame received.")

    @_if_connected
    def open_file(self, _: int) -> None:
        """
        File open function.

        Opens the HDF5 attenuation file.

        Args:
            _ (int): EPICS record processing value
        """
        if _ == 1:
            self.h5f._open_file()

        if self.file_close.get() != 0:
            self.file_close.set(0, process=False)

    @_if_connected
    def close_file(self, _: int) -> None:
        """
        File close function.

        Closes the HDF5 attenuation file.

        Args:
            _ (int): EPICS record processing value
        """
        if _ == 1:
            self.h5f._close_file()

        if self.file_open.get() != 0:
            self.file_open.set(0, process=False)

    def _req_status(self) -> None:
        """Send status request command to PowerBrick program."""
        req_status = b'{"command":"status"}'
        self._send_message(req_status)

    async def _query_status(self) -> None:
        """Query the status of the PowerBrick program every 0.1s."""
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

    def _handle_status(self, status: Dict[str, int]) -> None:
        """
        Handle status returned from PowerBrick program.

        Args:
            status (Dict[str, int]): Status dictionary returned from PowerBrick program
        """
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
        # Check that at least 1 frame has been received before timing out
        if time_since_last_frame > self.timeout_rbv.get() and last_received_frame > 1:
            self.close_file(1)

        current_attenuation = status["current_attenuation"]
        self.current_attenuation.set(current_attenuation)

    def _send_message(self, message: bytes) -> None:
        """
        Send ZMQ stream message.

        Args:
            message (bytes): Message to send over the zmq stream
        """
        self.zmq_stream.send_message([message])

    def _configure_param(
        self, param: Dict[str, Union[int, float, Dict[str, int]]]
    ) -> None:
        """
        Configure PowerBrick program parameter.

        Args:
            param (Dict[str, Union[int, float, Dict[str, int]]]): Parameter to configure
        """
        configure = json.dumps({"command": "configure", "params": param})
        self._send_message(codecs.encode(configure, "utf-8"))

    @_if_connected
    def _set_mode(self, mode: int) -> None:
        """
        Set mode of PMAC Filter Controller.

        Closes any open HDF5 file, and sets max attenuation if mode == Manual.

        Args:
            mode (int): Mode to set
        """
        # Set mode for PFC
        self._configure_param({"mode": mode})

        self.mode_rbv.set(mode)

        self.close_file(1)

        if mode == 0:
            self.attenuation.set(15)

    @_if_connected
    def _set_manual_attenuation(self, attenuation: int) -> None:
        """
        Set manual attenuation of PMAC Filter Controller.

        Args:
            attenuation (int): Attenuation to set
        """
        if self.state.get() == 0 and self.mode_rbv.get() == 0:
            # Set manual attenuation for PFC
            self._configure_param({"attenuation": attenuation})

        else:
            print("ERROR: Must be in MANUAL mode and IDLE state.")

    @_if_connected
    async def _reset(self, _: int) -> None:
        """
        Reset frame number of PMAC Filter Controller.

        Args:
            _ (int): EPICS record processing value
        """
        if _ == 1:
            reset = b'{"command":"reset"}'
            self._send_message(reset)

    @_if_connected
    def _set_timeout(self, timeout: int) -> None:
        """
        Set timeout of PMAC Filter Controller.

        Args:
            timeout (int): Timeout to set
        """
        self._configure_param({"timeout": timeout})

        self.timeout_rbv.set(timeout)

    @_if_connected
    async def _clear_error(self, _: int) -> None:
        """
        Clear error state of PMAC Filter Controller.

        Args:
            _ (int): EPICS record processing value
        """
        if _ == 1:
            clear_error = json.dumps({"command": "clear_error"})
            self._send_message(codecs.encode(clear_error, "utf-8"))

    @_if_connected
    async def _start_singleshot(self, _: int) -> None:
        """
        Trigger Singleshot logic in PMAC Filter Controller.

        Args:
            _ (int): EPICS record processing value
        """
        if _ == 1:
            if (
                self.state.get() == 3 or self.state.get() == 4
            ) and self.mode_rbv.get() == 2:
                start_singleshot = json.dumps({"command": "singleshot"})
                self._send_message(codecs.encode(start_singleshot, "utf-8"))
            else:
                print(
                    "ERROR: Must be in SINGLESHOT mode, and in \
                        SINGLESHOT_WAITING/COMPLETE state."
                )

    @_if_connected
    async def _set_shutter(self, shutter_state: int) -> None:
        """
        Set state of the fast shutter.

        Args:
            shutter_state (int): The state to set
        """
        if shutter_state == 0:  # SHUTTER_CLOSED
            pos = self.shutter_pos_closed.get()
        else:
            pos = self.shutter_pos_open.get()

        await caput(f"{self.motors}:SHUTTER", pos, wait=False, throw=False)

    def _set_shutter_pos(self, val: float, shutter_state: int) -> None:
        """
        Set the shutter position count value.

        Args:
            val (float): Count value of the shutter position
            shutter_state (str): The state of the shutter to set the position for
        """
        if shutter_state == SHUTTER_CLOSED:
            self._configure_param({"shutter_closed_position": val})

        current_shutter_state = (
            SHUTTER_CLOSED if self.shutter.get() == 0 else SHUTTER_OPEN
        )
        if current_shutter_state == shutter_state:
            self._set_shutter(shutter_state)

        self._autosave_dict[f"{self.device_name}:SHUTTER:{shutter_state}"] = val

        self.write_autosave()

    @_if_connected
    def _set_thresholds(self) -> None:
        """Set pixel threshold values for PMAC Filter Controller."""
        self._configure_param({"pixel_count_thresholds": self.pixel_count_thresholds})

        self.write_autosave()

    @_if_connected
    def _set_extreme_high_threshold(self, threshold: int) -> None:
        """
        Set extreme high pixel threshold value.

        Args:
            threshold (int): New threshold value
        """
        if threshold != self.pixel_count_thresholds["high3"]:
            self.pixel_count_thresholds["high3"] = threshold

	    self._autosave_dict[self.extreme_high_threshold.name] = threshold
            self._set_thresholds()

    @_if_connected
    def _set_upper_high_threshold(self, threshold: int) -> None:
        """
        Set upper high pixel threshold value.

        Args:
            threshold (int): New threshold value
        """
        if threshold != self.pixel_count_thresholds["high2"]:
            self.pixel_count_thresholds["high2"] = threshold

	    self._autosave_dict[self.upper_high_threshold.name] = threshold
            self._set_thresholds()

        else:
            print(f"High 2 is already at value {threshold}.")

    @_if_connected
    def _set_lower_high_threshold(self, threshold: int) -> None:
        """
        Set lower high pixel threshold value.

        Args:
            threshold (int): New threshold value
        """
        if threshold != self.pixel_count_thresholds["high1"]:
            self.pixel_count_thresholds["high1"] = threshold

	    self._autosave_dict[self.lower_high_threshold.name] = threshold
            self._set_thresholds()

        else:
            print(f"High 1 is already at value {threshold}.")

    @_if_connected
    def _set_upper_low_threshold(self, threshold: int) -> None:
        """
        Set upper low pixel threshold value.

        Args:
            threshold (int): New threshold value
        """
        if threshold != self.pixel_count_thresholds["low2"]:
            self.pixel_count_thresholds["low2"] = threshold

	    self._autosave_dict[self.upper_low_threshold.name] = threshold
            self._set_thresholds()

        else:
            print(f"Low 2 is already at value {threshold}.")

    @_if_connected
    def _set_lower_low_threshold(self, threshold: int) -> None:
        """
        Set lower low pixel threshold value.

        Args:
            threshold (int): New threshold value
        """
        if threshold != self.pixel_count_thresholds["low1"]:
            self.pixel_count_thresholds["low1"] = threshold

	    self._autosave_dict[self.lower_low_threshold.name] = threshold
            self._set_thresholds()

        else:
            print(f"Low 1 is already at value {threshold}.")

    @_if_connected
    async def _set_hist(self, hist_name: str, hist_val: int) -> None:
        """
        Set histogram threshold value.

        Args:
            hist_name (str): Name of the histogram threshold to set
            hist_val (int): Value to set the histogram threshold to
        """
        self._hist_thresholds[hist_name] = hist_val
        self._autosave_dict[hist_name] = hist_val
        await caput(
            f"{self.detector}:OD:SUM:Histogram:{hist_name}",
            hist_val,
        )

        self.write_autosave()

    @_if_connected
    async def _set_histogram_scale(self, scale: float) -> None:
        """
        Scale the histogram values by a factor.

        Args:
            scale (float): Scale factor
        """
        new_thresholds = self._hist_thresholds

        if scale != 1.0:
            await self._get_hist_thresholds()

            new_thresholds = {
                key: threshold * scale
                for key, threshold in self._hist_thresholds.items()
            }

        await self._set_hist_thresholds(new_thresholds)

    @_if_connected
    def _set_filter_set(self, filter_set_num: int) -> None:
        """
        Set filter set positions based on the filter set.

        Args:
            filter_set_num (int): Filter set to set positions for
        """
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

        if self.mode.get() > 0:
            self._set_mode(0)
            self.mode.set(0, process=False)
        else:
            self._set_manual_attenuation(15)

        self._autosave_dict[f"{self.device_name}:FILTER_SET"] = filter_set_num
        self.write_autosave()

    @_if_connected
    def _set_pos(self, filter_set: int, in_out_key: str, val: float) -> None:
        """
        Set position of a filter.

        Args:
            filter_set (int): Filter set of the filter
            in_out_key (str): The key to identify the filter in the filter set
            val (float): Position to set the filter position to
        """
        self._autosave_dict[f"{self.device_name}:{in_out_key}"] = val

        if self.filter_set_rbv.get() == filter_set - 1:
            self._set_filter_set(filter_set - 1)

        self.write_autosave()

    @_if_connected
    def _set_file_path(self, path: str) -> None:
        """
        Set file path of HDF5 attenuation file.

        Args:
            path (str): File path of HDF5 attenuation file
        """
        self.file_path_rbv.set(path)

        self._combine_file_path_and_name()

    @_if_connected
    def _set_file_name(self, name: str) -> None:
        """
        Set file name of HDF5 attenuation file.

        Args:
            name (str): File name to set
        """
        self.file_name_rbv.set(name)

        self._combine_file_path_and_name()

    def _combine_file_path_and_name(self) -> None:
        """Combine the file path and name into a full path."""
        path: str = self.file_path.get()
        name = self.file_name.get()

        full_path: str = "/".join([path, name])

        self.file_full_name.set(full_path)

        self.h5f._set_file_path(full_path)
