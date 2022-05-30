import os
from pathlib import Path


VENDOR = ('2022', 'Netcracker')
PROG_NAME = ('data_pump', 'Cloud Data Pump')
WORKING_DIR = Path('C:\\GRISHIN\\IT_NOTES\\PROJECTS\\py\\u02')
LOG_FILE = WORKING_DIR / 'logs' / 'data_pump.log'
CONFIG_FILE = WORKING_DIR / 'config.json'
MAPPING_FILE = WORKING_DIR / 'mapping.json'

BUILD_DATA = os.getenv('BUILD_DATE', '__')
HOST_WORKDIR = os.getenv('HOST_WORKING_DIR', '{WORKING_DIR}')

