from lxml.builder import ElementMaker

from sciencebeam_lab.svg_structured_document import (
  SvgStructuredDocument,
  SvgStyleClasses,
  SVG_NS
)

E = ElementMaker(namespace=SVG_NS)
SVG_TEXT_BLOCK = lambda *args: E.g({'class': 'block'}, *args)
SVG_TEXT_LINE = lambda *args: E.g({'class': 'line'}, *args)
SVG_TEXT = lambda *args: E.text(*args)

class TestSvgStructuredDocument(object):
  def test_should_return_root_as_pages(self):
    root = E.svg()
    doc = SvgStructuredDocument(root)
    assert list(doc.get_pages()) == [root]

  def test_should_find_lines_of_page_without_blocks(self):
    lines = [
      SVG_TEXT_LINE(),
      SVG_TEXT_LINE()
    ]
    doc = SvgStructuredDocument(
      E.svg(
        *lines
      )
    )
    page = doc.get_pages()[0]
    assert list(doc.get_lines_of_page(page)) == lines

  def test_should_find_lines_of_page_with_blocks(self):
    lines = [
      SVG_TEXT_LINE(),
      SVG_TEXT_LINE()
    ]
    doc = SvgStructuredDocument(
      E.svg(
        SVG_TEXT_BLOCK(
          *lines
        )
      )
    )
    page = doc.get_pages()[0]
    assert list(doc.get_lines_of_page(page)) == lines

  def test_should_find_tokens_of_line(self):
    tokens = [
      SVG_TEXT(),
      SVG_TEXT()
    ]
    line = SVG_TEXT_LINE(*tokens)
    doc = SvgStructuredDocument(
      E.svg(
        line,
        SVG_TEXT_LINE(SVG_TEXT())
      )
    )
    assert list(doc.get_tokens_of_line(line)) == tokens

  def test_should_tag_text_as_line_no(self):
    text = SVG_TEXT()
    doc = SvgStructuredDocument(
      E.svg(
        SVG_TEXT_LINE(text)
      )
    )
    doc.set_tag(text, SvgStyleClasses.LINE_NO)
    assert text.attrib['class'] == SvgStyleClasses.LINE_NO
