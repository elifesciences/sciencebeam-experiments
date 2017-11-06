from lxml.builder import E

from sciencebeam_lab.structured_document import (
  SimpleStructuredDocument,
  SimpleLine,
  SimpleToken
)

from sciencebeam_lab.matching_annotator import (
  MatchingAnnotator,
  TargetAnnotation,
  xml_root_to_target_annotations,
  THIN_SPACE,
  EN_DASH,
  EM_DASH,
  XmlMapping
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

  def test_should_apply_regex_to_result(self):
    xml_root = E.article(
      E.title('1.1. ' + SOME_VALUE)
    )
    xml_mapping = {
      'article': {
        TAG1: 'title',
        TAG1 + XmlMapping.REGEX_SUFFIX: r'(?:\d+\.?)* ?(.*)'
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

  def test_should_return_target_annotations_in_order_of_xml(self):
    xml_root = E.article(
      E.tag1('tag1.1'), E.tag2('tag2.1'), E.tag1('tag1.2'), E.tag2('tag2.2'),
    )
    xml_mapping = {
      'article': {
        TAG1: 'tag1',
        TAG2: 'tag2'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(ta.name, ta.value) for ta in target_annotations] == [
      (TAG1, 'tag1.1'), (TAG2, 'tag2.1'), (TAG1, 'tag1.2'), (TAG2, 'tag2.2')
    ]

class TestMatchingAnnotator(object):
  def test_should_not_fail_on_empty_document(self):
    doc = SimpleStructuredDocument(lines=[])
    MatchingAnnotator([]).annotate(doc)

  def test_should_not_fail_on_empty_line_with_blank_token(self):
    target_annotations = [
      TargetAnnotation('this is. matching', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[SimpleLine([
      SimpleToken('')
    ])])
    MatchingAnnotator(target_annotations).annotate(doc)

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

  def test_should_match_normalised_characters(self):
    matching_tokens = [
      SimpleToken('this'),
      SimpleToken('is' + THIN_SPACE + EN_DASH + EM_DASH),
      SimpleToken('matching')
    ]
    target_annotations = [
      TargetAnnotation('this is -- matching', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[SimpleLine(matching_tokens)])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_match_case_insensitive(self):
    matching_tokens = [
      SimpleToken('This'),
      SimpleToken('Is'),
      SimpleToken('Matching')
    ]
    target_annotations = [
      TargetAnnotation('tHIS iS mATCHING', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[SimpleLine(matching_tokens)])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_prefer_word_boundaries(self):
    pre_tokens = [
      SimpleToken('this')
    ]
    matching_tokens = [
      SimpleToken('is')
    ]
    post_tokens = [
      SimpleToken('miss')
    ]
    target_annotations = [
      TargetAnnotation('is', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[SimpleLine(
      pre_tokens + matching_tokens + post_tokens
    )])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(pre_tokens) == [None] * len(pre_tokens)
    assert _get_tags_of_tokens(post_tokens) == [None] * len(post_tokens)

  def test_should_annotate_multiple_value_target_annotation(self):
    matching_tokens = [
      SimpleToken('this'),
      SimpleToken('may'),
      SimpleToken('match')
    ]
    target_annotations = [
      TargetAnnotation([
        'this', 'may', 'match'
      ], TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[SimpleLine(matching_tokens)])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_annotate_multiple_value_target_annotation_over_multiple_lines(self):
    matching_tokens = [
      SimpleToken('this'),
      SimpleToken('may'),
      SimpleToken('match')
    ]
    tokens_by_line = [
      matching_tokens[0:1],
      matching_tokens[1:]
    ]
    target_annotations = [
      TargetAnnotation([
        'this', 'may', 'match'
      ], TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine(tokens) for tokens in tokens_by_line
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_annotate_not_match_distant_value_of_multiple_value_target_annotation(self):
    matching_tokens = [
      SimpleToken('this'),
      SimpleToken('may'),
      SimpleToken('match')
    ]
    distant_matching_tokens = [
      SimpleToken('not')
    ]
    distance_in_lines = 10
    tokens_by_line = [matching_tokens] + [
      [SimpleToken('other')] for _ in range(distance_in_lines)
    ] + [distant_matching_tokens]
    target_annotations = [
      TargetAnnotation([
        'this', 'may', 'match', 'not'
      ], TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine(tokens) for tokens in tokens_by_line
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(distant_matching_tokens) == [None] * len(distant_matching_tokens)

  def test_should_annotate_fuzzily_matching(self):
    matching_tokens = [
      SimpleToken('this'),
      SimpleToken('is'),
      SimpleToken('matching')
    ]
    target_annotations = [
      TargetAnnotation('this is. matching', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[SimpleLine(matching_tokens)])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_annotate_with_local_matching_smaller_gaps(self):
    matching_tokens = [
      SimpleToken('this'),
      SimpleToken('is'),
      SimpleToken('matching')
    ]
    target_annotations = [
      TargetAnnotation('this is. matching indeed matching', TAG1)
    ]
    # this should align with 'this is_ matching' with one gap'
    # instead of globally 'this is_ ________ ______ matching'
    # (which would result in a worse b_gap_ratio)
    doc = SimpleStructuredDocument(lines=[SimpleLine(matching_tokens)])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_not_annotate_fuzzily_matching_with_many_differences(self):
    matching_tokens = [
      SimpleToken('this'),
      SimpleToken('is'),
      SimpleToken('matching')
    ]
    target_annotations = [
      TargetAnnotation('txhxixsx ixsx mxaxtxcxhxixnxgx', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[SimpleLine(matching_tokens)])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [None] * len(matching_tokens)

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

  def test_should_annotate_over_multiple_lines_with_tag_transition(self):
    tag1_tokens_by_line = [
      [SimpleToken('this'), SimpleToken('may')],
      [SimpleToken('match')]
    ]
    tag1_tokens = flatten(tag1_tokens_by_line)
    tag2_tokens_by_line = [
      [SimpleToken('another')],
      [SimpleToken('tag'), SimpleToken('here')]
    ]
    tag2_tokens = flatten(tag2_tokens_by_line)
    tokens_by_line = [
      tag1_tokens_by_line[0],
      tag1_tokens_by_line[1] + tag2_tokens_by_line[0],
      tag2_tokens_by_line[1]
    ]
    target_annotations = [
      TargetAnnotation('this may match', TAG1),
      TargetAnnotation('another tag here', TAG2)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine(tokens) for tokens in tokens_by_line
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(tag1_tokens) == [TAG1] * len(tag1_tokens)
    assert _get_tags_of_tokens(tag2_tokens) == [TAG2] * len(tag2_tokens)

  def test_should_not_annotate_too_short_match_of_longer_sequence(self):
    matching_tokens = [
      SimpleToken('this'),
      SimpleToken('is'),
      SimpleToken('matching')
    ]
    too_short_tokens = [
      SimpleToken('1')
    ]
    tokens_per_line = [
      too_short_tokens,
      matching_tokens
    ]
    target_annotations = [
      TargetAnnotation('this is matching 1', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine(tokens) for tokens in tokens_per_line
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(too_short_tokens) == [None] * len(too_short_tokens)
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

  def test_should_not_override_annotation(self):
    matching_tokens_per_line = [
      [
        SimpleToken('this'),
        SimpleToken('is'),
        SimpleToken('matching')
      ]
    ]

    matching_tokens = flatten(matching_tokens_per_line)
    target_annotations = [
      TargetAnnotation('this is matching', TAG1),
      TargetAnnotation('matching', TAG2)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine(tokens)
      for tokens in matching_tokens_per_line
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

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

  def test_should_annotate_shorter_target_annotation_fuzzily(self):
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
      TargetAnnotation('this is. matching', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine(pre_tokens + matching_tokens + post_tokens)
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(pre_tokens) == [None] * len(pre_tokens)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(post_tokens) == [None] * len(post_tokens)

  def test_should_annotate_multiple_shorter_target_annotation_in_longer_line(self):
    pre_tokens = [
      SimpleToken('pre')
    ]
    matching_tokens_tag_1 = [
      SimpleToken('this'),
      SimpleToken('is'),
      SimpleToken('matching')
    ]
    mid_tokens = [
      SimpleToken('mid')
    ]
    matching_tokens_tag_2 = [
      SimpleToken('also'),
      SimpleToken('good')
    ]
    post_tokens = [
      SimpleToken('post')
    ]
    target_annotations = [
      TargetAnnotation('this is matching', TAG1),
      TargetAnnotation('also good', TAG2)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine(
        pre_tokens + matching_tokens_tag_1 + mid_tokens + matching_tokens_tag_2 + post_tokens
      )
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(pre_tokens) == [None] * len(pre_tokens)
    assert _get_tags_of_tokens(matching_tokens_tag_1) == [TAG1] * len(matching_tokens_tag_1)
    assert _get_tags_of_tokens(mid_tokens) == [None] * len(mid_tokens)
    assert _get_tags_of_tokens(matching_tokens_tag_2) == [TAG2] * len(matching_tokens_tag_2)
    assert _get_tags_of_tokens(post_tokens) == [None] * len(post_tokens)

  def test_should_not_annotate_shorter_target_annotation_in_longer_line_multiple_times(self):
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
    first_line_tokens = pre_tokens + matching_tokens + post_tokens
    similar_line_tokens = [
      SimpleToken(t.text) for t in first_line_tokens
    ]
    target_annotations = [
      TargetAnnotation('this is matching', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[
      SimpleLine(first_line_tokens),
      SimpleLine(similar_line_tokens)
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(similar_line_tokens) == [None] * len(similar_line_tokens)
