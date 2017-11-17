from lxml.builder import E

from sciencebeam_lab.utils.bounding_box import (
  BoundingBox
)

from sciencebeam_lab.lxml_structured_document import (
  LxmlStructuredDocument
)

class TestLxmlStructuredDocument(object):
  def test_should_find_pages(self):
    pages = [
      E.PAGE(),
      E.PAGE()
    ]
    doc = LxmlStructuredDocument(
      E.DOCUMENT(
        *pages
      )
    )
    assert list(doc.get_pages()) == pages

  def test_should_find_lines_of_page_without_blocks(self):
    lines = [
      E.TEXT(),
      E.TEXT()
    ]
    page = E.PAGE(*lines)
    doc = LxmlStructuredDocument(
      E.DOCUMENT(
        page,
        # add another page just for effect
        E.PAGE(
          E.TEXT()
        )
      )
    )
    assert list(doc.get_lines_of_page(page)) == lines

  def test_should_find_lines_of_page_with_blocks(self):
    lines = [
      E.TEXT(),
      E.TEXT()
    ]
    page = E.PAGE(E.BLOCK(*lines))
    doc = LxmlStructuredDocument(
      E.DOCUMENT(
        page,
        # add another page just for effect
        E.PAGE(
          E.BLOCK(E.TEXT())
        )
      )
    )
    assert list(doc.get_lines_of_page(page)) == lines

  def test_should_find_tokens_of_line(self):
    tokens = [
      E.TOKEN(),
      E.TOKEN()
    ]
    line = E.TEXT(*tokens)
    doc = LxmlStructuredDocument(
      E.DOCUMENT(
        E.PAGE(
          line,
          E.TEXT(E.TOKEN)
        )
      )
    )
    assert list(doc.get_tokens_of_line(line)) == tokens

  def test_should_calculate_default_bounding_box(self):
    token = E.TOKEN({
      'x': '10',
      'y': '11',
      'width': '100',
      'height': '101'
    })
    doc = LxmlStructuredDocument(E.DOCUMENT(E.PAGE(E.TEXT(token))))
    assert doc.get_bounding_box(token) == BoundingBox(10, 11, 100, 101)
