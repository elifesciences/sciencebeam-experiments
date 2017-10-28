from lxml.builder import E

from sciencebeam_lab.structured_document import (
  SimpleStructuredDocument,
  SimpleLine,
  SimpleToken
)

from sciencebeam_lab.matching_annotator import (
  MatchingAnnotator,
  TargetAnnotation,
  xml_root_to_target_annotations
)

from sciencebeam_lab.collection_utils import (
  flatten
)

TAG1 = 'tag1'
TAG2 = 'tag2'

SOME_VALUE = 'some value'

def _get_tags_of_tokens(tokens):
  return [t.get_tag() for t in tokens]

class TestXmlRootToTargetAnnotations(object):
  def test_should_return_empty_target_annotations_for_empty_xml(self):
    xml_root = E.article(
    )
    xml_mapping = {
      'article': {
        'title': 'title'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert target_annotations == []

  def test_should_return_empty_target_annotations_for_no_matching_annotations(self):
    xml_root = E.article(
      E.other(SOME_VALUE)
    )
    xml_mapping = {
      'article': {
        TAG1: 'title'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert target_annotations == []

  def test_should_return_matching_target_annotations(self):
    xml_root = E.article(
      E.title(SOME_VALUE)
    )
    xml_mapping = {
      'article': {
        TAG1: 'title'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert len(target_annotations) == 1
    assert target_annotations[0].name == TAG1
    assert target_annotations[0].value == SOME_VALUE

  def test_should_return_full_text(self):
    xml_root = E.article(
      E.title(
        'some ',
        E.other('embedded'),
        ' text'
      )
    )
    xml_mapping = {
      'article': {
        TAG1: 'title'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert len(target_annotations) == 1
    assert target_annotations[0].name == TAG1
    assert target_annotations[0].value == 'some embedded text'

class TestMatchingAnnotator(object):
  def test_should_not_fail_on_empty_document(self):
    doc = SimpleStructuredDocument(lines=[])
    MatchingAnnotator([]).annotate(doc)

  def test_should_annotate_exactly_matching(self):
    matching_tokens = [
      SimpleToken('this'),
      SimpleToken('is'),
      SimpleToken('matching')
    ]
    target_annotations = [
      TargetAnnotation('this is matching', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[SimpleLine(matching_tokens)])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_not_annotate_not_matching(self):
    not_matching_tokens = [
      SimpleToken('something'),
      SimpleToken('completely'),
      SimpleToken('different')
    ]
    target_annotations = [
      TargetAnnotation('this is matching', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[SimpleLine(not_matching_tokens)])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(not_matching_tokens) == [None] * len(not_matching_tokens)

  def test_should_annotate_exactly_matching_across_multiple_lines(self):
    matching_tokens_per_line = [
      [
        SimpleToken('this'),
        SimpleToken('is'),
        SimpleToken('matching')
      ],
      [
        SimpleToken('and'),
        SimpleToken('continues'),
        SimpleToken('here')
      ]
    ]
    matching_tokens = flatten(matching_tokens_per_line)
    target_annotations = [
      TargetAnnotation('this is matching and continues here', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine(tokens) for tokens in matching_tokens_per_line
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_not_annotate_similar_sequence_multiple_times(self):
    matching_tokens_per_line = [
      [
        SimpleToken('this'),
        SimpleToken('is'),
        SimpleToken('matching')
      ],
      [
        SimpleToken('and'),
        SimpleToken('continues'),
        SimpleToken('here')
      ]
    ]
    not_matching_tokens = [
      SimpleToken('this'),
      SimpleToken('is'),
      SimpleToken('matching')
    ]

    matching_tokens = flatten(matching_tokens_per_line)
    target_annotations = [
      TargetAnnotation('this is matching and continues here', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine(tokens)
      for tokens in matching_tokens_per_line + [not_matching_tokens]
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(not_matching_tokens) == [None] * len(not_matching_tokens)

  def test_should_not_annotate_pre_annotated_tokens_on_separate_lines(self):
    line_no_tokens = [SimpleToken('1')]
    line_no_tokens[0].set_tag('line_no')
    matching_tokens = [
      SimpleToken('this'),
      SimpleToken('is'),
      SimpleToken('matching')
    ]
    target_annotations = [
      TargetAnnotation('1', TAG2),
      TargetAnnotation('this is matching', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine(line_no_tokens),
      SimpleLine(matching_tokens)
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(line_no_tokens) == ['line_no'] * len(line_no_tokens)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_annotate_shorter_target_annotation_in_longer_line(self):
    pre_tokens = [
      SimpleToken('pre')
    ]
    matching_tokens = [
      SimpleToken('this'),
      SimpleToken('is'),
      SimpleToken('matching')
    ]
    post_tokens = [
      SimpleToken('post')
    ]
    target_annotations = [
      TargetAnnotation('this is matching', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine(pre_tokens + matching_tokens + post_tokens)
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(pre_tokens) == [None] * len(pre_tokens)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(post_tokens) == [None] * len(post_tokens)
