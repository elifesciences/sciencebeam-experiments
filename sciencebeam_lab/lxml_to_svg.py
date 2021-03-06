import argparse
import logging
import os

from lxml import etree

SVG_NS = 'http://www.w3.org/2000/svg'
SVG_NS_PREFIX = '{' + SVG_NS + '}'
SVG_DOC = SVG_NS_PREFIX + 'svg'
SVG_TEXT = SVG_NS_PREFIX + 'text'
SVG_G = SVG_NS_PREFIX + 'g'

SVG_NSMAP = {
  None : SVG_NS
}

class SvgStyleClasses(object):
  LINE = 'line'
  BLOCK = 'block'

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

def iter_svg_pages_for_lxml(lxml_root):
  previous_block = None
  previous_svg_block = None
  for page in lxml_root.xpath('//DOCUMENT/PAGE'):
    svg_root = etree.Element(SVG_DOC, nsmap=SVG_NSMAP)
    for text in page.xpath('.//TEXT'):
      svg_g = etree.Element(SVG_G, nsmap=SVG_NSMAP, attrib={
        'class': SvgStyleClasses.LINE
      })
      for token in text.xpath('./TOKEN'):
        x = float(token.attrib.get('x'))
        y = float(token.attrib.get('y'))
        height = float(token.attrib.get('height'))
        base = float(token.attrib.get('base', y))
        y_center = y + height / 2.0
        attrib = {
          'x': x,
          'y': base,
          'font-size': token.attrib.get('font-size'),
          'font-family': token.attrib.get('font-name'),
          'fill': token.attrib.get('font-color')
        }
        angle = float(token.attrib.get('angle', '0'))
        if token.attrib.get('rotation') == '1' and angle == 90.0:
          attrib['x'] = '0'
          attrib['y'] = '0'
          attrib['transform'] = 'translate({x} {y}) rotate({angle})'.format(
            x=x,
            y=y_center,
            angle=-angle
          )
        svg_g.append(
          _create_xml_node(SVG_TEXT, token.text, attrib=attrib)
        )
      text_parent = text.getparent()
      if text_parent.tag == 'BLOCK':
        if text_parent != previous_block:
          previous_svg_block = etree.Element(SVG_G, nsmap=SVG_NSMAP, attrib={
            'class': SvgStyleClasses.BLOCK
          })
          svg_root.append(previous_svg_block)
          previous_block = text_parent
        previous_svg_block.append(svg_g)
      else:
        previous_block = None
        previous_svg_block = None
        svg_root.append(svg_g)
    yield svg_root

def main():
  logger = get_logger()
  args = parse_args()
  svg_filename_pattern = args.svg_path
  if not svg_filename_pattern:
    svg_filename_pattern = svg_pattern_for_lxml_path(args.lxml_path)
  logger.debug('svg_filename_pattern: %s', svg_filename_pattern)
  lxml_root = etree.parse(args.lxml_path).getroot()
  for page_index, svg_root in enumerate(iter_svg_pages_for_lxml(lxml_root)):
    svg_filename = svg_filename_pattern.format(1 + page_index)
    logger.info('writing to: %s', svg_filename)
    with open(svg_filename, 'wb') as f:
      etree.ElementTree(svg_root).write(f, pretty_print=True)

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)

  main()
