import os
from pathlib import Path

# require('pygelf')
# Import the basic framework components.
from softioc import asyncio_dispatcher, builder, softioc

from pmacfiltercontrol.pmacFilterControlWrapper import (
    Wrapper as pmacFilterControlWrapper,
)

# A couple of identification PVs, assumes this file is the name of the IOC
device_name = "pytest_pfcw"
builder.SetDeviceName(device_name)
builder.stringIn("WHOAMI", initial_value="Test Fast Attenutator Control")
builder.stringIn("HOSTNAME", VAL=os.uname()[1])

filter_set_total = 2
filters_per_set = 2

autosave_pos_file = f"{Path.cwd()}/tests/test_autosave.txt"

dispatcher = asyncio_dispatcher.AsyncioDispatcher()

wrapper = pmacFilterControlWrapper(
    "127.0.0.1",
    9000,
    9001,
    builder=builder,
    device_name=device_name,
    filter_set_total=filter_set_total,
    filters_per_set=filters_per_set,
    detector="BLXXI-EA-EXCBR-01",
    motors="BLXXI-OP-FILT-01",
    autosave_file_path=autosave_pos_file,
    hdf_file_path=f"{Path.cwd()}/tests/",
)

# dispatcher(wrapper.run_forever)

# setup_logging(default_level=logging.DEBUG)

# Now get the IOC started
builder.LoadDatabase()
softioc.iocInit(dispatcher)

# wrapper.set_device_info()

# Leave the iocsh running
softioc.interactive_ioc(globals())
