from __future__ import absolute_import

import logging
import csv
from io import BytesIO

from six import string_types

import apache_beam as beam
from apache_beam.io.textio import WriteToText

from sciencebeam_lab.beam_utils.utils import (
  TransformAndLog
)

from sciencebeam_lab.utils.csv import (
  csv_delimiter_by_filename
)

def get_logger():
  return logging.getLogger(__name__)

def DictToList(fields):
  def wrapper(x):
    get_logger().debug('DictToList: %s -> %s', fields, x)
    return [x.get(field) for field in fields]
  return wrapper

def format_csv_rows(rows, delimiter=','):
  get_logger().debug('format_csv_rows, rows: %s', rows)
  out = BytesIO()
  writer = csv.writer(out, delimiter=delimiter)
  writer.writerows([
    [
      x.encode('utf-8') if isinstance(x, string_types) else x
      for x in row
    ]
    for row in rows
  ])
  result = out.getvalue().decode('utf-8').rstrip('\r\n')
  get_logger().debug('format_csv_rows, result: %s', result)
  return result

class WriteDictCsv(beam.PTransform):
  def __init__(self, path, columns, file_name_suffix=None):
    super(WriteDictCsv, self).__init__()
    self.path = path
    self.columns = columns
    self.file_name_suffix = file_name_suffix
    self.delimiter = csv_delimiter_by_filename(path + file_name_suffix)

  def expand(self, pcoll):
    return (
      pcoll |
      "ToList" >> beam.Map(DictToList(self.columns)) |
      "Format" >> TransformAndLog(
        beam.Map(lambda x: format_csv_rows([x], delimiter=self.delimiter)),
        log_prefix='formatted csv: ',
        log_level='debug'
      ) |
      "Utf8Encode" >> beam.Map(lambda x: x.encode('utf-8')) |
      "Write" >> WriteToText(
        self.path,
        file_name_suffix=self.file_name_suffix,
        header=format_csv_rows([self.columns], delimiter=self.delimiter).encode('utf-8')
      )
    )
