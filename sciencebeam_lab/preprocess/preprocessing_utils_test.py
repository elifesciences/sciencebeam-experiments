from mock import patch, DEFAULT

from lxml import etree

from sciencebeam_lab.structured_document.svg import (
  SVG_DOC
)

from sciencebeam_lab.preprocess.preprocessing_utils import (
  svg_page_to_blockified_png_bytes
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
