import logging
import os
import sys

from enum import Enum


class LOG_LEVEL_TYPE(Enum):
  info="info"
  warning="warning"
  critical="critical"
  error="error"


class LOGGER_TYPE(Enum):
  stdout_only="stdout_only"
  app_log="app_log"
  transaction_log="transaction_log"





IO_TRANSACTION_LOG_DIR = os.environ.get("IO_TRANSACTION_LOG_DIR", "/mnt/trn")


def setup_logger_stdout(name):
  formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S')

  screen_handler = logging.StreamHandler(stream=sys.stdout)
  screen_handler.setFormatter(formatter)
  logger = logging.getLogger(name)
  logger.setLevel(logging.DEBUG)
  logger.addHandler(screen_handler)
  return logger


def setup_logger_stdout_and_file(name, file_path=os.path.join(
  IO_TRANSACTION_LOG_DIR, "log.txt")):
  formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S')
  handler = logging.FileHandler(file_path,'w')
  handler.setFormatter(formatter)
  screen_handler = logging.StreamHandler(stream=sys.stdout)
  screen_handler.setFormatter(formatter)
  logger = logging.getLogger(name)
  logger.setLevel(logging.DEBUG)
  logger.addHandler(handler)
  logger.addHandler(screen_handler)
  return logger





def get_default_logger(logger_type=LOGGER_TYPE.stdout_only):
  if logger_type==LOGGER_TYPE.app_log:
    return APP_LOGGER

  if logger_type==LOGGER_TYPE.transaction_log:
    return TRANSACTION_LOGGER

  return STDOUT_LOGGER




TRANSACTION_LOGGER=setup_logger_stdout_and_file("transaction_file_out_logger",
                                        file_path=os.path.join(
                                          IO_TRANSACTION_LOG_DIR,
                                          "transaction.log"))

APP_LOGGER=setup_logger_stdout_and_file("applog",
                                        file_path=os.path.join(
                                          IO_TRANSACTION_LOG_DIR,
                                          "fio-integrity.log"))

STDOUT_LOGGER=setup_logger_stdout("stdout_logger")



def log_message(message,logger_type=LOGGER_TYPE.app_log,level=LOG_LEVEL_TYPE.info):

  logger=get_default_logger(logger_type=logger_type)
  func=getattr(logger,level.value)
  func(message)

