from sciencebeam_lab.structured_document import (
  AbstractStructuredDocument
)

class LxmlStructuredDocument(AbstractStructuredDocument):
  def __init__(self, root):
    self.root = root

  def get_pages(self):
    return self.root.findall('.//PAGE')

  def get_lines_of_page(self, parent):
    return parent.findall('.//TEXT')

  def get_tokens_of_line(self, line):
    return line.findall('./TOKEN')

  def get_x(self, parent):
    return parent.attrib.get('x')

  def get_text(self, parent):
    return parent.text

  def set_tag(self, parent, tag):
    parent.attrib['tag'] = tag
