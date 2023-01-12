import subprocess
import os
from os.path import exists

import json
import time

"/mnt/data/last_written_pattern"
from dataclasses import dataclass


@dataclass
class IOConfig:
  io_mode: str
  io_patterns: list
  io_file: str
  io_write_step: int
  iteration_sleep: int
  io_file_size: str


def fio_execute(fio_command):
  try:
    result = subprocess.run(fio_command, shell=True, check=True,
                            capture_output=True,
                            text=True)
    print("fio command {} executed successfully output: {}".format(fio_command,
                                                                   result.stdout))
    return True
  except subprocess.CalledProcessError as e:
    print("CRITICAL - fio command failed.")
    print("STDOUT: {}".format(e.output))
    print("STDERR: {}".format(e.stderr))
    print("return code: {}".format(e.returncode))
    print("command: {}".format(e.cmd))
    raise Exception("fio command failed!")


def load_config():
  config_file_path = os.environ.get("IO_CONFIG_PATH",
                                    "/mnt/config/config.json")
  with open(config_file_path, 'r') as config_file:
    config = json.load(config_file)
    config_file.close()

  io_mode = config.get("io_mode", "write")
  io_patterns = config.get("io_patterns", ["0x0123456789abcdef"])
  io_file = config.get("io_file", "/mnt/data/file1")

  io_write_step = config.get("io_write_step", 2)

  iteration_sleep = config.get("iteration_sleep", 1)
  io_file_size = config.get("io_file_size", "5Gi")

  io_fill_percent = config.get("io_fill_percent", None)

  io_config = IOConfig(io_mode=io_mode,
                       io_patterns=io_patterns,
                       io_file=io_file,
                       io_write_step=io_write_step,
                       iteration_sleep=iteration_sleep,
                       io_file_size=io_file_size)

  return io_config


def fio_init():
  # if contianer has ENV: Init - we assume its init container. Primary
  # Container should not have INIT env- directly skip to loop
  io_config = load_config()
  if exists("/mnt/data/wal"):
    if exists("/mnt/data/last_written_pattern"):
      with open("/mnt/data/last_written_pattern", "r") as \
        last_written_pattern_file:
        last_written_pattern = last_written_pattern_file.read()
        last_written_pattern_file.close()
      print("WARNING: WAL file found - assuming previous pod crashed midway - "
            "re-writing with last_written_pattern: {}".format(
        last_written_pattern))
      io_config = load_config()
      run_fio_write(last_written_pattern, io_config)

    else:
      raise Exception(
        "CRITICAL: WAL file found but no last_written_pattern - check "
        "previous container logs to find out why")
    # performing verify irrepsective of re-write based on WAL or not

  elif exists("/mnt/data/last_written_pattern"):
    run_fio_verify(io_config)

  elif exists(io_config.io_file):
    raise Exception(
      "CRITICAL: last_written_pattern not found but data file found- check "
      "previous container logs to find out why.")
  else:
    print("No last_written_pattern, WAL or data file found. Assuming this is "
          "the first run. moving to the loop writes")


def fio_loop():
  i = 0
  while True:
    io_config = load_config()
    print("-" * 10)
    print("Iteration: {}".format(i))

    if io_config.io_mode == "write":
      print("Write Mode is enabled. Checking if it is the write iteration")
      if not (i % io_config.io_write_step):
        current_pattern_index = (i % len(io_config.io_patterns)) - 1  # loop
        current_pattern = io_config.io_patterns[current_pattern_index]
        run_fio_write(current_pattern, io_config=io_config)

        i += 1
        continue

    i += 1
    run_fio_verify(io_config)
    time.sleep(io_config.iteration_sleep)


def run_fio_write(current_pattern: str, io_config: IOConfig):
  try:
    with open("/mnt/data/wal", 'w') as wal_file:
      wal_file.write(current_pattern)
      wal_file.close()

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
          "--overwrite=1".format(io_config.io_file,  io_config.io_file_size,
                                                        current_pattern)

    fio_execute(cmd)

    with open("/mnt/data/last_written_pattern", 'w') as last_written_file:
      last_written_file.write(current_pattern)
      last_written_file.close()
    os.remove("/mnt/data/wal")

    print("-" * 10)

    print("Write Run Complete. Pattern: {0}. IO Config: {1}".format(
      current_pattern, io_config))

    return True
  except Exception as inst:
    print(type(inst))
    print(inst.args)
    print(inst)
    exit(code=1)


def run_fio_verify(io_config: IOConfig):
  try:
    if exists("/data/wal"):
      raise Exception("WAL file found - cannot verify - crashing now")

    with open("/mnt/data/last_written_pattern", 'r') as last_written_pattern:
      pattern = last_written_pattern.read()
      print("INFO: Found last written pattern: {}. Verifying it now".format(
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
          "--overwrite=1".format(io_config.io_file, pattern)

    fio_execute(cmd)

    print("-" * 10)

    print("Verify Run Complete. Pattern: {0}. IO Config: {1}".format(
      last_written_pattern, io_config))

    return True



  except Exception as inst:
    print(type(inst))
    print(inst.args)
    print(inst)
    exit(code=1)


if __name__ == '__main__':
  if os.environ.get("INIT", None):
    fio_init()
  else:
    fio_loop()
