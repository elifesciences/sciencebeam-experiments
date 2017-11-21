from contextlib import contextmanager
import logging
from mock import Mock, patch, DEFAULT

import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from sciencebeam_lab.utils.collection import (
  extend_dict
)

from sciencebeam_lab.beam_utils.utils import (
  TransformAndLog
)

from sciencebeam_lab.preprocess.preprocessing_pipeline import (
  parse_args,
  configure_pipeline
)

TestPipeline.__test__ = False

PREPROCESSING_PIPELINE = 'sciencebeam_lab.preprocess.preprocessing_pipeline'

BASE_DATA_PATH = 'base'
PDF_PATH = '*/*.pdf'
XML_PATH = '*/*.xml'

PDF_FILE_1 = '1/file.pdf'
XML_FILE_1 = '1/file.xml'

def get_logger():
  return logging.getLogger(__name__)

def fake_content(path):
  return 'fake content: %s' % path

def fake_lxml_for_pdf(pdf, path):
  return 'fake lxml for pdf: %s (%s)' % (pdf, path)

fake_svg_page = lambda i=0: 'fake svg page: %d' % i
fake_pdf_png_page = lambda i=0: 'fake pdf png page: %d' % i
fake_block_png_page = lambda i=0: 'fake block png page: %d' % i

_global_tfrecords_mock = Mock(name='_global_tfrecords_mock')

def get_global_tfrecords_mock():
  # workaround for mock that would get serialized/deserialized before being invoked
  global _global_tfrecords_mock
  return _global_tfrecords_mock

@contextmanager
def patch_preprocessing_pipeline(**kwargs):
  def DummyWritePropsToTFRecord(file_path, extract_props):
    return TransformAndLog(beam.Map(
      lambda v: get_global_tfrecords_mock()(file_path, list(extract_props(v)))
    ), log_fn=lambda x: get_logger().info('tfrecords: %s', x))

  always_mock = {
    'find_file_pairs_grouped_by_parent_directory',
    'pdf_bytes_to_png_pages',
    'convert_and_annotate_lxml_content',
    'svg_page_to_blockified_png_bytes',
    'save_svg_roots',
    'save_pages',
    'evaluate_document_by_page'
  }

  with patch.multiple(
    PREPROCESSING_PIPELINE,
    read_all_from_path=fake_content,
    convert_pdf_bytes_to_lxml=fake_lxml_for_pdf,
    WritePropsToTFRecord=DummyWritePropsToTFRecord,
    **{
      k: kwargs.get(k, DEFAULT)
      for k in always_mock
    }
  ) as mocks:
    # mocks['read_all_from_path'] = lambda path: fake_content(path)
    yield extend_dict(
      mocks,
      {'tfrecords': get_global_tfrecords_mock()}
    )

def get_default_args():
  return parse_args([
    '--data-path=' + BASE_DATA_PATH,
    '--pdf-path=' + PDF_PATH,
    '--xml-path=' + XML_PATH
  ])

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

  def test_should_write_tfrecords(self):
    with patch_preprocessing_pipeline() as mocks:
      opt = get_default_args()
      opt.save_tfrecords = True
      with TestPipeline() as p:
        mocks['find_file_pairs_grouped_by_parent_directory'].return_value = [
          (PDF_FILE_1, XML_FILE_1)
        ]
        mocks['convert_and_annotate_lxml_content'].return_value = [
          fake_svg_page(1)
        ]
        mocks['pdf_bytes_to_png_pages'].return_value = [
          fake_pdf_png_page(1)
        ]
        mocks['svg_page_to_blockified_png_bytes'].return_value = fake_block_png_page(1)
        configure_pipeline(p, opt)

      mocks['tfrecords'].assert_called_with(opt.output_path + '/data', [{
        'input_uri': PDF_FILE_1,
        'annotation_uri': PDF_FILE_1 + '.annot',
        'input_image': fake_pdf_png_page(1),
        'annotation_image': fake_block_png_page(1)
      }])

  def test_should_write_multiple_tfrecords(self):
    with patch_preprocessing_pipeline() as mocks:
      opt = get_default_args()
      opt.save_tfrecords = True
      with TestPipeline() as p:
        mocks['find_file_pairs_grouped_by_parent_directory'].return_value = [
          (PDF_FILE_1, XML_FILE_1)
        ]
        mocks['convert_and_annotate_lxml_content'].return_value = [
          fake_svg_page(i) for i in [1, 2]
        ]
        mocks['pdf_bytes_to_png_pages'].return_value = [
          fake_pdf_png_page(i) for i in [1, 2]
        ]
        mocks['svg_page_to_blockified_png_bytes'].side_effect = [
          fake_block_png_page(1),
          fake_block_png_page(2)
        ]
        configure_pipeline(p, opt)

      mocks['tfrecords'].assert_called_with(opt.output_path + '/data', [{
        'input_uri': PDF_FILE_1,
        'annotation_uri': PDF_FILE_1 + '.annot',
        'input_image': fake_pdf_png_page(i),
        'annotation_image': fake_block_png_page(i)
      } for i in [1, 2]])

  def test_should_not_write_tfrecord_below_annotation_threshold(self):
    custom_mocks = dict(
      evaluate_document_by_page=lambda _: [{
        'percentage': {
          # low percentage of None (no annotation, include)
          None: 0.1
        }
      }, {
        'percentage': {
          # low percentage of None (no annotation, exclude)
          None: 0.9
        }
      }]
    )
    with patch_preprocessing_pipeline(**custom_mocks) as mocks:
      opt = get_default_args()
      opt.save_tfrecords = True
      opt.min_annotation_percentage = 0.5
      with TestPipeline() as p:
        mocks['find_file_pairs_grouped_by_parent_directory'].return_value = [
          (PDF_FILE_1, XML_FILE_1)
        ]
        mocks['convert_and_annotate_lxml_content'].return_value = [
          fake_svg_page(i) for i in [1, 2]
        ]
        mocks['pdf_bytes_to_png_pages'].return_value = [
          fake_pdf_png_page(i) for i in [1, 2]
        ]
        mocks['svg_page_to_blockified_png_bytes'].side_effect = [
          fake_block_png_page(1),
          fake_block_png_page(2)
        ]
        configure_pipeline(p, opt)

      mocks['tfrecords'].assert_called_with(opt.output_path + '/data', [{
        'input_uri': PDF_FILE_1,
        'annotation_uri': PDF_FILE_1 + '.annot',
        'input_image': fake_pdf_png_page(i),
        'annotation_image': fake_block_png_page(i)
      } for i in [1]])

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
