import subprocess
import os
from os.path import exists
from custom_logging import *

import json
import time
import custom_logging
import sys
from dataclasses import dataclass, field

from enum import Enum


# Default Params. All of below params can be controlling by changing the
# config.json

IO_CONFIG_PATH = os.environ.get("IO_CONFIG_PATH", "/mnt/config/config.json")
IO_PATTERNS = "0x0123456789abcdef"
IO_DATA_PATH = os.environ.get("IO_DATA_PATH", "/mnt/data")
IO_FILE_NAME = "file1"
IO_WRITE_STEP = 2
ITERATION_SLEEP = 1
IO_FILE_SIZE = "5Gi"
IO_CRASH_ON_WAL_DETECTION = False






@dataclass
class IOConfig:
  io_mode: str
  io_patterns: list
  io_data_path: str
  io_transaction_log_dir: str
  io_file_name: str
  io_write_step: int
  iteration_sleep: int
  io_file_size: str
  io_file_path: str = field(init=False)
  io_wal_file_path: str = field(init=False)
  io_last_written_pattern: str = field(init=False)
  io_crash_on_wal_detection: bool = IO_CRASH_ON_WAL_DETECTION

  def __post_init__(self):
    self.io_file_path = os.path.join(self.io_data_path, self.io_file_name)
    self.io_wal_file_path = os.path.join(self.io_transaction_log_dir, "wal")
    self.io_last_written_pattern = os.path.join(self.io_transaction_log_dir,
                                                "io_last_written_pattern")













def os_command_execute(command_str, logger_type=LOGGER_TYPE.app_log):
  try:
    result = subprocess.run(command_str, shell=True, check=True,
                            capture_output=True,
                            text=True)
    log_message("command {} executed successfully output: {}".format(
      command_str,      result.stdout),logger_type=logger_type,
      level=LOG_LEVEL_TYPE.info)
    return True
  except subprocess.CalledProcessError as e:

    log_message("FAILED command {} - STDOUT: {} - STDERR: {}".format(
      command_str, e.stdout,e.stderr), logger_type=logger_type,
      level=LOG_LEVEL_TYPE.critical)
    raise Exception("EXCEPTION: command failed! - {}".format(e.cmd))



def load_config():
  config_file_path = os.environ.get("IO_CONFIG_PATH", IO_CONFIG_PATH)
  with open(config_file_path, 'r') as config_file:
    config = json.load(config_file)
    config_file.close()

  io_mode = config.get("io_mode", "write")
  io_patterns = config.get("io_patterns", [IO_PATTERNS])
  io_data_path = config.get("io_data_path", IO_DATA_PATH)
  io_transaction_log_dir = config.get("io_transaction_log_dir",
                                      IO_TRANSACTION_LOG_DIR)
  io_file_name = config.get("io_file_name", IO_FILE_NAME)

  io_write_step = config.get("io_write_step", IO_WRITE_STEP)

  iteration_sleep = config.get("iteration_sleep", ITERATION_SLEEP)
  io_file_size = config.get("io_file_size", IO_FILE_SIZE)
  io_crash_on_wal_detection = config.get("io_crash_on_wal_detection", IO_CRASH_ON_WAL_DETECTION)

  io_config = IOConfig(io_mode=io_mode,
                       io_patterns=io_patterns,
                       io_file_name=io_file_name,
                       io_write_step=io_write_step,
                       iteration_sleep=iteration_sleep,
                       io_file_size=io_file_size,
                       io_transaction_log_dir=io_transaction_log_dir,
                       io_data_path=io_data_path)
  log_message("IO Config generated: {}".format(io_config))
  return io_config


def fio_init():
  io_config = load_config()
  log_message("Init Process - Verifying data and transaction directory")
  log_message("Printing data directory structure")
  os_command_execute("ls -l {}".format(io_config.io_data_path))

  if exists(io_config.io_wal_file_path):
    if io_config.io_crash_on_wal_detection:
      log_message(
        "io_crash_on_wal_detection is set to TRUE and WAL file detected - "
        "crashing now",level=LOG_LEVEL_TYPE.critical)
      raise Exception(
        "CRITICAL: WAL file found on init and flag io_crash_on_wal_detection "
        "set to true ")

    if exists(io_config.io_last_written_pattern):

      log_message("last_written_pattern file exists")
      with open(io_config.io_last_written_pattern, "r") as \
        last_written_pattern_file:
        last_written_pattern = last_written_pattern_file.read()
        last_written_pattern_file.close()
      log_message("WARNING: WAL file found - assuming previous pod "
                          "crashed midway "
                          "and flag io_crash_on_wal_detection set to false."
                          "Re-writing with last_written_pattern: {}".format(
        last_written_pattern),level=LOG_LEVEL_TYPE.error)
      io_config = load_config()
      log_message(
        "Rerunning the io pattern as pod crashed with WAL "
        "entry",level=LOG_LEVEL_TYPE.error)
      run_fio_write(last_written_pattern, io_config)
      return True

    else:
      raise Exception(
        "CRITICAL: WAL file found but no last_written_pattern - check "
        "previous container logs to find out why")

    # performing verify irrepsective of re-write based on WAL or not

  log_message("WAL does not exist. Assuming pod did not crash mid-way of a "
               "write op")

  log_message("Checking if last_written_pattern exists. If WAL is not "
               "present and "
        "last write pattern exists - we assume the last run was a read verify and we are going to re-run the verify op")
  if exists(io_config.io_last_written_pattern):
    log_message("last_written_pattern exists. verifying now")
    if (run_fio_verify(io_config)):
      log_message("verify complete - exiting the init stage")
      return True
    else:
      raise Exception("CRITICAL: Verify Error")

  log_message("last written pattern file does not exist. Meaning both WAL and last "
        "written pattern does not exist")

  log_message("Checking if data file is found. If WAL and/or last written pattern "
        "does not exist but the data file is present - we assume something "
        "wrong as happened and crash")

  if exists(io_config.io_file_path):
    raise Exception(
      "CRITICAL: last_written_pattern not found but data file found- check "
      "previous container logs to find out why.")


  log_message("No last_written_pattern, WAL or data file found. Assuming this is "
        "the first run. moving to the loop writes")
  return True

def fio_loop():


  log_message("-" * 10)
  log_message("BEGINNING the fio integrity loop")
  log_message("-" * 10)
  i = 0
  while True:

    io_config = load_config()
    log_message("-" * 10)
    log_message("Iteration: {}".format(i))

    if io_config.io_mode == "write":
      log_message("Write Mode is enabled. Checking if it is the write iteration")
      if not (i % io_config.io_write_step):
        log_message("Write Mode is enabled and it is the write iteration. "
              "Performing write op")

        current_pattern_index = (i % len(io_config.io_patterns)) - 1  # loop
        current_pattern = io_config.io_patterns[current_pattern_index]
        run_fio_write(current_pattern, io_config=io_config)

        i += 1
        continue
      log_message(
        "Write Mode enabled and it is NOT the write iteration. Performing verify op")

    i += 1
    log_message("Performing Verify Op")
    run_fio_verify(io_config)
    log_message("verify complete")
    log_message("sleeping %s seconds".format(io_config.iteration_sleep))
    time.sleep(int(io_config.iteration_sleep))


def run_fio_write(current_pattern: str, io_config: IOConfig):
  try:
    with open(io_config.io_wal_file_path, 'w') as wal_file:

      wal_file.write(current_pattern)
      wal_file.close()
      log_message("WAL file written with Pattern {}".format(current_pattern),
                  logger_type=LOGGER_TYPE.transaction_log)

    cmd = "fio  " \
          "--name=rand " \
          "--filename={0} " \
          "--size='{1}' " \
          "--rw=write " \
          "--bs=128k" \
          "--ioengine=libaio " \
          "--iodepth=4 " \
          "--verify=pattern " \
          "--do_verify=0 " \
          "--verify_pattern='{2}' " \
          "--overwrite=1".format(io_config.io_file_path, io_config.io_file_size,
                                 current_pattern)

    os_command_execute(cmd)

    log_message("Data written to file/disk. Updating last_written_pattern",
                logger_type=LOGGER_TYPE.transaction_log)


    with open(io_config.io_last_written_pattern, 'w') as last_written_file:
      last_written_file.write(current_pattern)

      last_written_file.close()
      log_message("last written pattern updated",
                  logger_type=LOGGER_TYPE.transaction_log)
    os.remove(io_config.io_wal_file_path)
    log_message("WAL file removed. Marking transaction success",
                logger_type=LOGGER_TYPE.transaction_log)


    log_message("-" * 10)

    log_message("Write Run Complete. Pattern: {0}. IO Config: {1}".format(
      current_pattern, io_config),logger_type=LOGGER_TYPE.transaction_log)

    return True
  except Exception as inst:
    log_message("FIO write failed. Traceback below ",
                logger_type=LOGGER_TYPE.transaction_log,
                level=LOG_LEVEL_TYPE.critical)
    log_message(type(inst), logger_type=LOGGER_TYPE.transaction_log,
                level=LOG_LEVEL_TYPE.critical)
    log_message(type(inst.args), logger_type=LOGGER_TYPE.transaction_log,
                level=LOG_LEVEL_TYPE.critical)
    log_message(inst.args, logger_type=LOGGER_TYPE.transaction_log,
                level=LOG_LEVEL_TYPE.critical)

    raise Exception("FIO write failure!!. Check transaction logs")






def run_fio_verify(io_config: IOConfig):
  try:
    if exists(io_config.io_wal_file_path):
      raise Exception("WAL file found - cannot verify - crashing now")


    with open(io_config.io_last_written_pattern, 'r') as last_written_pattern:
      pattern = last_written_pattern.read()
      if not pattern:
        raise Exception("CRITICAL last_written_pattern not found in verify op")

      log_message("INFO: Found last written pattern: {}. Verifying it now".format(
        pattern))
      last_written_pattern.close()

    cmd = "fio " \
          "--name=rand " \
          "--filename={0}  " \
          "--bs=128k " \
          "--rw=read " \
          "--ioengine=libaio " \
          "--iodepth=4 " \
          "--verify=pattern " \
          "--do_verify=1 " \
          "--verify_pattern='{1}' " \
          "--overwrite=1".format(io_config.io_file_path, pattern)

    os_command_execute(cmd)

    log_message("-" * 10)

    log_message("Verify Run Complete. Pattern: {0}. IO Config: {1}".format(
      last_written_pattern, io_config))

    return True



  except Exception as inst:
    log_message("FIO verify failed. Traceback below ",
                logger_type=LOGGER_TYPE.transaction_log,
                level=LOG_LEVEL_TYPE.critical)
    log_message(type(inst), logger_type=LOGGER_TYPE.transaction_log,
                level=LOG_LEVEL_TYPE.critical)
    log_message(type(inst.args), logger_type=LOGGER_TYPE.transaction_log,
                level=LOG_LEVEL_TYPE.critical)
    log_message(inst.args, logger_type=LOGGER_TYPE.transaction_log,
                level=LOG_LEVEL_TYPE.critical)
    raise E


if __name__ == '__main__':

  if not os.environ.get("INIT", None):
    log_message("ENV variable INIT  not detected - running init check and "
                "then proceeding to loop")
    if not fio_init():
      log_message("CRITICAL FIO INIT failed. crashing now",
                  level=LOG_LEVEL_TYPE.critical)

    log_message("init process complete - proceeding to loop read/verify")
    fio_loop()
  else:
    log_message("ENV variable INIT detected - running init  process")
    fio_init()
