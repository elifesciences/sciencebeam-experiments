from sciencebeam_lab.structured_document import (
  SimpleStructuredDocument,
  SimpleDocument,
  SimpleLine,
  SimpleToken
)

from sciencebeam_lab.find_line_number import (
  find_line_number_tokens
)

class TestFindLxmlLineNumberTokens(object):
  def test_should_return_empty_list_for_empty_page(self):
    doc = SimpleStructuredDocument(SimpleDocument([
    ]))
    line_number_tokens = list(find_line_number_tokens(doc))
    assert len(line_number_tokens) == 0

  def test_should_return_line_number_tokens_appearing_first_in_line(self):
    line_number_tokens = [
      SimpleToken(str(line_no), dict(
        x=str(10),
        y=str(line_no * 20))
      )
      for line_no in range(1, 5)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine([
        line_number_token,
        SimpleToken('other text', dict(
          x=str(50),
          y=line_number_token.get_y()
        ))
      ])
      for line_number_token in line_number_tokens
    ])
    expected_line_number_tokens = line_number_tokens
    actual_line_number_tokens = list(find_line_number_tokens(doc))
    assert actual_line_number_tokens == expected_line_number_tokens

  def test_should_not_return_line_number_tokens_if_not_line(self):
    line_number_tokens = [
      SimpleToken(str(line_no), dict(
        x=str(30),
        y=str(line_no * 20))
      )
      for line_no in range(1, 5)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine([
        line_number_token,
        SimpleToken('other text', dict(
          x=str(20),
          y=line_number_token.get_y()
        ))
      ])
      for line_number_token in line_number_tokens
    ])
    expected_line_number_tokens = []
    actual_line_number_tokens = list(find_line_number_tokens(doc))
    assert actual_line_number_tokens == expected_line_number_tokens

  def test_should_not_return_line_number_tokens_at_unusual_position(self):
    usual_line_number_x = 1
    line_number_tokens = [
      SimpleToken(str(line_no), dict(
        x=str(usual_line_number_x if line_no != 2 else usual_line_number_x + 30),
        y=str(line_no * 20))
      )
      for line_no in range(1, 5)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine([
        line_number_token,
        SimpleToken('other text', dict(
          x=str(50),
          y=line_number_token.get_y()
        ))
      ])
      for line_number_token in line_number_tokens
    ])
    expected_line_number_tokens = [
      t for t in line_number_tokens
      if int(t.get_x()) == usual_line_number_x
    ]
    actual_line_number_tokens = list(find_line_number_tokens(doc))
    assert actual_line_number_tokens == expected_line_number_tokens
