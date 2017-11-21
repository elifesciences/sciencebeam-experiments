from contextlib import contextmanager
from mock import Mock, patch, DEFAULT

import pytest

from apache_beam.testing.test_pipeline import TestPipeline

from sciencebeam_lab.preprocess.preprocessing_pipeline import (
  parse_args,
  configure_pipeline
)

TestPipeline.__test__ = False

PREPROCESSING_PIPELINE = 'sciencebeam_lab.preprocess.preprocessing_pipeline'

def patch_preprocessing_pipeline():
  return patch.multiple(
    PREPROCESSING_PIPELINE,
    find_file_pairs_grouped_by_parent_directory=DEFAULT
  )

def get_default_args():
  return parse_args(['--data-path=test', '--pdf-path=test', '--xml-path=test'])

@pytest.mark.filterwarnings('ignore::DeprecationWarning')
@pytest.mark.filterwarnings('ignore::UserWarning')
class TestConfigurePipeline(object):
  def test_should_pass_pdf_and_xml_patterns_to_find_file_pairs_grouped_by_parent_directory(self):
    with patch_preprocessing_pipeline() as mocks:
      opt = get_default_args()
      opt.base_data_path = 'base'
      opt.pdf_path = 'pdf'
      opt.xml_path = 'xml'
      with TestPipeline() as p:
        mocks['find_file_pairs_grouped_by_parent_directory'].return_value = []
        configure_pipeline(p, opt)

      mocks['find_file_pairs_grouped_by_parent_directory'].assert_called_with(
        ['base/pdf', 'base/xml']
      )

  def test_should_pass_lxml_and_xml_patterns_to_find_file_pairs_grouped_by_parent_directory(self):
    with patch_preprocessing_pipeline() as mocks:
      opt = get_default_args()
      opt.base_data_path = 'base'
      opt.pdf_path = ''
      opt.lxml_path = 'lxml'
      opt.xml_path = 'xml'
      with TestPipeline() as p:
        mocks['find_file_pairs_grouped_by_parent_directory'].return_value = []
        configure_pipeline(p, opt)

      mocks['find_file_pairs_grouped_by_parent_directory'].assert_called_with(
        ['base/lxml', 'base/xml']
      )

class TestParseArgs(object):
  def test_should_raise_error_without_arguments(self):
    with pytest.raises(SystemExit):
      parse_args([])

  def test_should_not_raise_error_with_minimum_arguments(self):
    parse_args(['--data-path=test', '--pdf-path=test', '--xml-path=test'])

  def test_should_not_raise_error_with_lxml_path_instead_of_pdf_path(self):
    parse_args(['--data-path=test', '--lxml-path=test', '--xml-path=test'])

  def test_should_raise_error_if_pdf_and_lxml_path_are_specified(self):
    with pytest.raises(SystemExit):
      parse_args(['--data-path=test', '--pdf-path=test', '--lxml-path=test', '--xml-path=test'])

  def test_should_not_raise_error_with_save_lxml_path_together_with_pdf_path(self):
    parse_args(['--data-path=test', '--pdf-path=test', '--save-lxml', '--xml-path=test'])

  def test_should_raise_error_if_save_lxml_specified_without_pdf_path(self):
    with pytest.raises(SystemExit):
      parse_args(['--data-path=test', '--lxml-path=test', '--save-lxml', '--xml-path=test'])

  def test_should_raise_error_if_save_png_is_specified_without_pdf_path(self):
    with pytest.raises(SystemExit):
      parse_args(['--data-path=test', '--lxml-path=test', '--save-png', '--xml-path=test'])

  def test_should_raise_error_if_image_width_was_specified_without_image_height(self):
    with pytest.raises(SystemExit):
      parse_args(['--data-path=test', '--pdf-path=test', '--xml-path=test', '--image-width=100'])

  def test_should_raise_error_if_image_height_was_specified_without_image_width(self):
    with pytest.raises(SystemExit):
      parse_args(['--data-path=test', '--pdf-path=test', '--xml-path=test', '--image-height=100'])

  def test_should_not_raise_error_if_both_image_width_and_height_are_specified(self):
    parse_args([
      '--data-path=test', '--pdf-path=test', '--xml-path=test',
      '--image-width=100', '--image-height=100'
    ])

  def test_should_raise_error_if_save_tfrecords_specified_without_pdf_path(self):
    with pytest.raises(SystemExit):
      parse_args(['--data-path=test', '--lxml-path=test', '--xml-path=test', '--save-tfrecords'])

  def test_should_not_raise_error_if_save_tfrecords_specified_with_pdf_path(self):
    parse_args(['--data-path=test', '--pdf-path=test', '--xml-path=test', '--save-tfrecords'])
