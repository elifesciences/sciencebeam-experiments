from sciencebeam_lab.structured_document import (
  AbstractStructuredDocument
)

SVG_NS = 'http://www.w3.org/2000/svg'
SVG_NS_PREFIX = '{' + SVG_NS + '}'
SVG_DOC = SVG_NS_PREFIX + 'svg'
SVG_TEXT = SVG_NS_PREFIX + 'text'
SVG_G = SVG_NS_PREFIX + 'g'

SVG_TAG_ATTRIB = 'class'

SVG_NSMAP = {
  None : SVG_NS
}

class SvgStyleClasses(object):
  LINE = 'line'
  BLOCK = 'block'
  LINE_NO = 'line_no'

class SvgStructuredDocument(AbstractStructuredDocument):
  def __init__(self, root):
    self.root = root

  def get_pages(self):
    return [self.root]

  def get_lines_of_page(self, page):
    return page.findall('.//{}[@class="{}"]'.format(SVG_G, SvgStyleClasses.LINE))

  def get_tokens_of_line(self, line):
    return line.findall('./{}'.format(SVG_TEXT))

  def get_x(self, parent):
    return parent.attrib.get('x')

  def get_text(self, parent):
    return parent.text

  def set_tag(self, parent, tag):
    parent.attrib[SVG_TAG_ATTRIB] = tag
