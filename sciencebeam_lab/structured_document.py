from abc import ABCMeta, abstractmethod

class AbstractStructuredDocument(object):
  __metaclass__ = ABCMeta

  @abstractmethod
  def get_pages(self):
    pass

  @abstractmethod
  def get_lines_of_page(self, page):
    pass

  @abstractmethod
  def get_tokens_of_line(self, line):
    pass

  @abstractmethod
  def get_x(self, parent):
    pass

  @abstractmethod
  def get_text(self, parent):
    pass

  @abstractmethod
  def set_tag(self, parent, tag):
    pass

class SimpleToken(object):
  def __init__(self, text, attrib):
    self.text = text
    self.attrib = attrib

  def get_x(self):
    return self.attrib.get('x')

  def get_y(self):
    return self.attrib.get('y')

  def get_tag(self):
    return self.attrib.get('tag')

  def get_text(self):
    return self.text

  def set_tag(self, tag):
    self.attrib['tag'] = tag

class SimpleLine(object):
  def __init__(self, tokens):
    self.tokens = tokens

class SimpleDocument(object):
  def __init__(self, lines):
    self.lines = lines

class SimpleStructuredDocument(AbstractStructuredDocument):
  def __init__(self, root=None, lines=None):
    if lines is not None:
      root = SimpleDocument(lines)
    self.root = root

  def get_pages(self):
    return [self.root]

  def get_lines(self):
    return self.get_lines_of(self.root)

  def get_lines_of_page(self, page):
    return page.lines

  def get_tokens_of_line(self, line):
    return line.tokens

  def get_x(self, parent):
    return parent.get_x()

  def get_text(self, parent):
    return parent.get_text()

  def set_tag(self, parent, tag):
    return parent.set_tag(tag)
