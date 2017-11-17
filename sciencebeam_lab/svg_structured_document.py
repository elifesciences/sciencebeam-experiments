from sciencebeam_lab.utils.bounding_box import (
  BoundingBox
)

from sciencebeam_lab.structured_document import (
  AbstractStructuredDocument
)

SVG_NS = 'http://www.w3.org/2000/svg'
SVG_NS_PREFIX = '{' + SVG_NS + '}'
SVG_DOC = SVG_NS_PREFIX + 'svg'
SVG_TEXT = SVG_NS_PREFIX + 'text'
SVG_G = SVG_NS_PREFIX + 'g'
SVG_RECT = SVG_NS_PREFIX + 'rect'

SVG_TAG_ATTRIB = 'class'

SVG_NSMAP = {
  None : SVG_NS
}

class SvgStyleClasses(object):
  LINE = 'line'
  BLOCK = 'block'
  LINE_NO = 'line_no'

def get_node_bounding_box(t):
  attrib = t.attrib
  if not ('font-size' in attrib and 'x' in attrib and 'y' in attrib):
    return None
  font_size = float(attrib['font-size'])
  width = font_size * 0.8 * max(1, len(t.text))
  return BoundingBox(
    float(attrib['x']),
    float(attrib['y']),
    width,
    font_size
  )

class SvgStructuredDocument(AbstractStructuredDocument):
  def __init__(self, root_or_roots):
    if isinstance(root_or_roots, list):
      self.page_roots = root_or_roots
    else:
      self.page_roots = [root_or_roots]

  def get_pages(self):
    return self.page_roots

  def get_lines_of_page(self, page):
    return page.findall('.//{}[@class="{}"]'.format(SVG_G, SvgStyleClasses.LINE))

  def get_tokens_of_line(self, line):
    return line.findall('./{}'.format(SVG_TEXT))

  def get_x(self, parent):
    return parent.attrib.get('x')

  def get_text(self, parent):
    return parent.text

  def get_tag(self, parent):
    return parent.attrib.get(SVG_TAG_ATTRIB)

  def set_tag(self, parent, tag):
    parent.attrib[SVG_TAG_ATTRIB] = tag

  def get_bounding_box(self, parent):
    return get_node_bounding_box(parent)

  def set_bounding_box(self, parent, bounding_box):
    raise RuntimeError('not implemented')
