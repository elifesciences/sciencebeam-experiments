import argparse
import logging
import os

from lxml import etree

SVG_NS = 'http://www.w3.org/2000/svg'
SVG_NS_PREFIX = '{' + SVG_NS + '}'
SVG_DOC = SVG_NS_PREFIX + 'svg'
SVG_TEXT = SVG_NS_PREFIX + 'text'

SVG_NSMAP = {
  None : SVG_NS
}


def get_logger():
  return logging.getLogger(__name__)

def _create_xml_node(tag, text=None, attrib=None):
  node = etree.Element(tag)
  if text is not None:
    node.text = text
  if attrib is not None:
    for k, v in attrib.items():
      node.attrib[k] = str(v)
  return node

def svg_pattern_for_lxml_path(lxml_path):
  name, _ = os.path.splitext(lxml_path)
  return name + '-path{}.svg'

def parse_args(argv=None):
  parser = argparse.ArgumentParser(
    description='Convert to LXML (pdftoxml) to SVG'
  )
  parser.add_argument(
    '--lxml-path', type=str, required=True,
    help='path to lxml file'
  )
  parser.add_argument(
    '--svg-path', type=str, required=False,
    help='path to svg file'
  )
  args = parser.parse_args(argv)
  return args

def main():
  logger = get_logger()
  args = parse_args()
  svg_filename_pattern = args.svg_path
  if not svg_filename_pattern:
    svg_filename_pattern = svg_pattern_for_lxml_path(args.lxml_path)
  logger.debug('svg_filename_pattern: %s', svg_filename_pattern)
  lxml_root = etree.parse(args.lxml_path).getroot()
  for page_index, page in enumerate(lxml_root.xpath('//DOCUMENT/PAGE')):
    svg_root = etree.Element(SVG_DOC, nsmap=SVG_NSMAP)
    for token in page.xpath('./TEXT/TOKEN'):
      svg_root.append(
        _create_xml_node(SVG_TEXT, token.text, attrib={
          'x': token.attrib.get('x'),
          'y': token.attrib.get('base'),
          'font-size': token.attrib.get('font-size'),
          'font-family': token.attrib.get('font-name'),
          'fill': token.attrib.get('font-color')
        })
      )
    svg_filename = svg_filename_pattern.format(1 + page_index)
    logger.info('writing to: %s', svg_filename)
    with open(svg_filename, 'wb') as f:
      etree.ElementTree(svg_root).write(f, pretty_print=True)

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)

  main()
