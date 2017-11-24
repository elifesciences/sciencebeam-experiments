from mock import patch, DEFAULT

from lxml import etree

from sciencebeam_lab.structured_document.svg import (
  SVG_DOC
)

from sciencebeam_lab.preprocess.preprocessing_utils import (
  svg_page_to_blockified_png_bytes,
  group_file_pairs_by_parent_directory_or_name
)

PROCESSING_UTILS = 'sciencebeam_lab.preprocess.preprocessing_utils'

class TestSvgPageToBlockifiedPngBytes(object):
  def test_should_parse_viewbox_and_pass_width_and_height_to_annotated_blocks_to_image(self):
    with patch.multiple(PROCESSING_UTILS, annotated_blocks_to_image=DEFAULT) as mocks:
      svg_page = etree.Element(SVG_DOC, attrib={
        'viewBox': '0 0 100.1 200.9'
      })
      color_map = {}
      image_size = (100, 200)
      svg_page_to_blockified_png_bytes(svg_page, color_map, image_size)
      call_args = mocks['annotated_blocks_to_image'].call_args
      kwargs = call_args[1]
      assert (kwargs.get('width'), kwargs.get('height')) == (100.1, 200.9)

class TestGroupFilePairsByParentDirectoryOrName(object):
  def test_should_return_empty_list_with_empty_input_file_lists(self):
    assert list(group_file_pairs_by_parent_directory_or_name([
      [],
      []
    ])) == []

  def test_should_group_single_file(self):
    assert list(group_file_pairs_by_parent_directory_or_name([
      ['parent1/file.x'],
      ['parent1/file.y']
    ])) == [('parent1/file.x', 'parent1/file.y')]

  def test_should_group_single_file_in_directory_with_different_names(self):
    assert list(group_file_pairs_by_parent_directory_or_name([
      ['parent1/file1.x'],
      ['parent1/file2.y']
    ])) == [('parent1/file1.x', 'parent1/file2.y')]

  def test_should_ignore_files_in_different_directories(self):
    assert list(group_file_pairs_by_parent_directory_or_name([
      ['parent1/file.x'],
      ['parent2/file.y']
    ])) == []

  def test_should_group_multiple_files_in_separate_parent_directories(self):
    assert list(group_file_pairs_by_parent_directory_or_name([
      ['parent1/file.x', 'parent2/file.x'],
      ['parent1/file.y', 'parent2/file.y']
    ])) == [
      ('parent1/file.x', 'parent1/file.y'),
      ('parent2/file.x', 'parent2/file.y')
    ]

  def test_should_group_multiple_files_in_same_parent_directory_with_same_name(self):
    assert list(group_file_pairs_by_parent_directory_or_name([
      ['parent1/file1.x', 'parent1/file2.x'],
      ['parent1/file1.y', 'parent1/file2.y']
    ])) == [
      ('parent1/file1.x', 'parent1/file1.y'),
      ('parent1/file2.x', 'parent1/file2.y')
    ]

  def test_should_group_multiple_files_in_same_parent_directory_with_same_name_gzipped(self):
    assert list(group_file_pairs_by_parent_directory_or_name([
      ['parent1/file1.x.gz', 'parent1/file2.x.gz'],
      ['parent1/file1.y.gz', 'parent1/file2.y.gz']
    ])) == [
      ('parent1/file1.x.gz', 'parent1/file1.y.gz'),
      ('parent1/file2.x.gz', 'parent1/file2.y.gz')
    ]
