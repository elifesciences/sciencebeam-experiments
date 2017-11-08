from lxml.builder import E

from sciencebeam_lab.xml_utils import (
  get_text_content
)

SOME_VALUE_1 = 'some value1'
SOME_VALUE_2 = 'some value2'

class TestGetTextContent(object):
  def test_should_return_simple_text(self):
    node = E.parent(SOME_VALUE_1)
    assert get_text_content(node) == SOME_VALUE_1

  def test_should_return_text_of_child_element(self):
    node = E.parent(E.child(SOME_VALUE_1))
    assert get_text_content(node) == SOME_VALUE_1

  def test_should_return_text_of_child_element_and_preceeding_text(self):
    node = E.parent(SOME_VALUE_1, E.child(SOME_VALUE_2))
    assert get_text_content(node) == SOME_VALUE_1 + SOME_VALUE_2

  def test_should_return_text_of_child_element_and_trailing_text(self):
    node = E.parent(E.child(SOME_VALUE_1), SOME_VALUE_2)
    assert get_text_content(node) == SOME_VALUE_1 + SOME_VALUE_2

  def test_should_return_text_of_parent_excluding_children_to_exclude(self):
    child = E.child(SOME_VALUE_1)
    node = E.parent(child, SOME_VALUE_2)
    assert get_text_content(node, exclude=[child]) == SOME_VALUE_2
