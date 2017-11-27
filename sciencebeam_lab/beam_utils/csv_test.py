from __future__ import absolute_import

from contextlib import contextmanager
from mock import patch

import apache_beam as beam
from apache_beam.testing.util import (
  assert_that,
  equal_to
)

from sciencebeam_lab.beam_utils.testing import (
  TestPipeline,
  BeamTest,
  MockWriteToText,
  MockReadFromText
)

from sciencebeam_lab.beam_utils.csv import (
  WriteDictCsv,
  ReadDictCsv,
  format_csv_rows
)

MODULE_UNDER_TEST = 'sciencebeam_lab.beam_utils.csv'

@contextmanager
def patch_module_under_test(**kwargs):
  with patch.multiple(
    MODULE_UNDER_TEST,
    **kwargs
  ) as mocks:
    yield mocks

def to_csv(rows, delimiter):
  return format_csv_rows(rows, delimiter).encode('utf-8').replace('\r\n', '\n') + '\n'

class TestWriteDictCsv(BeamTest):
  def test_should_write_tsv_with_header(self, test_context):
    with patch_module_under_test(WriteToText=MockWriteToText):
      with TestPipeline() as p:
        _ = (
          p |
          beam.Create([{
            'a': 'a1',
            'b': 'b1'
          }]) |
          WriteDictCsv(
            '.temp/dummy',
            ['a', 'b'],
            '.tsv'
          )
        )
      assert test_context.get_file_content('.temp/dummy.tsv') == to_csv([
        ['a', 'b'],
        ['a1', 'b1']
      ], '\t')

class TestReadDictCsv(BeamTest):
  def test_should_read_rows_as_dict(self, test_context):
    with patch_module_under_test(ReadFromText=MockReadFromText):
      test_context.set_file_content('.temp/dummy.tsv', to_csv([
        ['a', 'b'],
        ['a1', 'b1']
      ], '\t'))

      with TestPipeline() as p:
        result = (
          p |
          ReadDictCsv('.temp/dummy.tsv')
        )
        assert_that(result, equal_to([{
          'a': 'a1',
          'b': 'b1'
        }]))
