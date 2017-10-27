from sciencebeam_lab.structured_document import (
  SimpleStructuredDocument,
  SimpleLine,
  SimpleToken
)

from sciencebeam_lab.matching_annotator import (
  MatchingAnnotator,
  TargetAnnotation
)

from sciencebeam_lab.collection_utils import (
  flatten
)

TAG1 = 'tag1'
TAG2 = 'tag2'

def _get_tags_of_tokens(tokens):
  return [t.get_tag() for t in tokens]

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
