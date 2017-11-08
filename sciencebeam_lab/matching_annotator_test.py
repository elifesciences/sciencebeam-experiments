import json

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
SOME_VALUE_2 = 'some value2'
SOME_LONGER_VALUE = 'some longer value1'
SOME_SHORTER_VALUE = 'value1'

def _get_tags_of_tokens(tokens):
  return [t.get_tag() for t in tokens]

def _copy_tokens(tokens):
  return [SimpleToken(t.text) for t in tokens]

def _tokens_for_text(text):
  return [SimpleToken(s) for s in text.split(' ')]

def _lines_for_tokens(tokens_by_line):
  return [SimpleLine(tokens) for tokens in tokens_by_line]

def _document_for_tokens(tokens_by_line):
  return SimpleStructuredDocument(lines=_lines_for_tokens(tokens_by_line))

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

  def test_should_apply_match_multiple_flag(self):
    xml_root = E.article(
      E.title(SOME_VALUE)
    )
    xml_mapping = {
      'article': {
        TAG1: 'title',
        TAG1 + XmlMapping.MATCH_MULTIPLE: 'true'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [t.match_multiple for t in target_annotations] == [True]

  def test_should_not_apply_match_multiple_flag_if_not_set(self):
    xml_root = E.article(
      E.title(SOME_VALUE)
    )
    xml_mapping = {
      'article': {
        TAG1: 'title'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [t.match_multiple for t in target_annotations] == [False]

  def test_should_apply_match_bonding_flag(self):
    xml_root = E.article(
      E.title(SOME_VALUE)
    )
    xml_mapping = {
      'article': {
        TAG1: 'title',
        TAG1 + XmlMapping.BONDING: 'true'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [t.bonding for t in target_annotations] == [True]

  def test_should_not_apply_match_bonding_flag_if_not_set(self):
    xml_root = E.article(
      E.title(SOME_VALUE)
    )
    xml_mapping = {
      'article': {
        TAG1: 'title'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [t.bonding for t in target_annotations] == [False]

  def test_should_use_multiple_xpaths(self):
    xml_root = E.article(
      E.entry(
        E.child1(SOME_VALUE),
        E.child2(SOME_VALUE_2)
      )
    )
    xml_mapping = {
      'article': {
        TAG1: '\n{}\n{}\n'.format(
          'entry/child1',
          'entry/child2'
        )
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(t.name, t.value) for t in target_annotations] == [
      (TAG1, SOME_VALUE),
      (TAG1, SOME_VALUE_2)
    ]

  def test_should_apply_children_xpaths_and_sort_by_value_descending(self):
    xml_root = E.article(
      E.entry(
        E.child1(SOME_SHORTER_VALUE),
        E.child2(SOME_LONGER_VALUE)
      ),
      E.entry(
        E.child1(SOME_LONGER_VALUE)
      )
    )
    xml_mapping = {
      'article': {
        TAG1: 'entry',
        TAG1 + XmlMapping.CHILDREN: './/*'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(t.name, t.value) for t in target_annotations] == [
      (TAG1, [SOME_LONGER_VALUE, SOME_SHORTER_VALUE]),
      (TAG1, SOME_LONGER_VALUE)
    ]

  def test_should_apply_children_xpaths_and_exclude_parents(self):
    xml_root = E.article(
      E.entry(
        E.parent(
          E.child2(SOME_LONGER_VALUE),
          E.child1(SOME_SHORTER_VALUE)
        )
      )
    )
    xml_mapping = {
      'article': {
        TAG1: 'entry',
        TAG1 + XmlMapping.CHILDREN: './/*'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(t.name, t.value) for t in target_annotations] == [
      (TAG1, [SOME_LONGER_VALUE, SOME_SHORTER_VALUE])
    ]

  def test_should_apply_multiple_children_xpaths_and_include_parent_text_if_enabled(self):
    xml_root = E.article(
      E.entry(
        E.child1(SOME_SHORTER_VALUE),
        SOME_LONGER_VALUE
      )
    )
    xml_mapping = {
      'article': {
        TAG1: 'entry',
        TAG1 + XmlMapping.CHILDREN: '\n{}\n{}\n'.format('.//*', '.'),
        TAG1 + XmlMapping.UNMATCHED_PARENT_TEXT: 'true'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(t.name, t.value) for t in target_annotations] == [
      (TAG1, [SOME_LONGER_VALUE, SOME_SHORTER_VALUE])
    ]

  def test_should_apply_concat_children(self):
    num_values = ['101', '202']
    xml_root = E.article(
      E.entry(
        E.parent(
          E.child1(SOME_VALUE),
          E.fpage(num_values[0]),
          E.lpage(num_values[1])
        )
      )
    )
    xml_mapping = {
      'article': {
        TAG1: 'entry',
        TAG1 + XmlMapping.CHILDREN: './/*',
        TAG1 + XmlMapping.CHILDREN_CONCAT: json.dumps([[{
          'xpath': './/fpage'
        }, {
          'value': '-'
        }, {
          'xpath': './/lpage'
        }]])
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(t.name, t.value) for t in target_annotations] == [
      (TAG1, [SOME_VALUE, '-'.join(num_values)])
    ]

  def test_should_not_apply_concat_children_if_one_node_was_not_found(self):
    num_values = ['101', '202']
    xml_root = E.article(
      E.entry(
        E.parent(
          E.child1(SOME_VALUE),
          E.fpage(num_values[0]),
          E.lpage(num_values[1])
        )
      )
    )
    xml_mapping = {
      'article': {
        TAG1: 'entry',
        TAG1 + XmlMapping.CHILDREN: './/*',
        TAG1 + XmlMapping.CHILDREN_CONCAT: json.dumps([[{
          'xpath': './/fpage'
        }, {
          'value': '-'
        }, {
          'xpath': './/unknown'
        }]])
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(t.name, t.value) for t in target_annotations] == [
      (TAG1, [SOME_VALUE, num_values[0], num_values[1]])
    ]

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
    doc = _document_for_tokens([[SimpleToken('')]])
    MatchingAnnotator(target_annotations).annotate(doc)

  def test_should_annotate_exactly_matching(self):
    matching_tokens = _tokens_for_text('this is matching')
    target_annotations = [
      TargetAnnotation('this is matching', TAG1)
    ]
    doc = _document_for_tokens([matching_tokens])
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
    doc = _document_for_tokens([matching_tokens])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_match_case_insensitive(self):
    matching_tokens = _tokens_for_text('This Is Matching')
    target_annotations = [
      TargetAnnotation('tHIS iS mATCHING', TAG1)
    ]
    doc = SimpleStructuredDocument(lines=[SimpleLine(matching_tokens)])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_prefer_word_boundaries(self):
    pre_tokens = _tokens_for_text('this')
    matching_tokens = _tokens_for_text('is')
    post_tokens = _tokens_for_text('miss')
    target_annotations = [
      TargetAnnotation('is', TAG1)
    ]
    doc = _document_for_tokens([
      pre_tokens + matching_tokens + post_tokens
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(pre_tokens) == [None] * len(pre_tokens)
    assert _get_tags_of_tokens(post_tokens) == [None] * len(post_tokens)

  def test_should_annotate_multiple_value_target_annotation(self):
    matching_tokens = _tokens_for_text('this may match')
    target_annotations = [
      TargetAnnotation([
        'this', 'may', 'match'
      ], TAG1)
    ]
    doc = _document_for_tokens([matching_tokens])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_annotate_multiple_value_target_annotation_over_multiple_lines(self):
    tokens_by_line = [
      _tokens_for_text('this may'),
      _tokens_for_text('match')
    ]
    matching_tokens = flatten(tokens_by_line)
    target_annotations = [
      TargetAnnotation([
        'this', 'may', 'match'
      ], TAG1)
    ]
    doc = _document_for_tokens(tokens_by_line)
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_annotate_not_match_distant_value_of_multiple_value_target_annotation(self):
    matching_tokens = _tokens_for_text('this may match')
    distant_matching_tokens = _tokens_for_text('not')
    distance_in_lines = 10
    tokens_by_line = [matching_tokens] + [
      _tokens_for_text('other') for _ in range(distance_in_lines)
    ] + [distant_matching_tokens]
    target_annotations = [
      TargetAnnotation([
        'this', 'may', 'match', 'not'
      ], TAG1)
    ]
    doc = _document_for_tokens(tokens_by_line)
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(distant_matching_tokens) == [None] * len(distant_matching_tokens)

  def test_should_annotate_not_match_distant_value_of_target_annotation_with_bonding(self):
    matching_tokens = _tokens_for_text('this may match')
    distant_matching_tokens = _tokens_for_text('not')
    distance_in_lines = 10
    tokens_by_line = [matching_tokens] + [
      _tokens_for_text('other') for _ in range(distance_in_lines)
    ] + [distant_matching_tokens]
    target_annotations = [
      TargetAnnotation('this may match', TAG1, bonding=True),
      TargetAnnotation('not', TAG1, bonding=True)
    ]
    doc = _document_for_tokens(tokens_by_line)
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(distant_matching_tokens) == [None] * len(distant_matching_tokens)

  def test_should_annotate_fuzzily_matching(self):
    matching_tokens = _tokens_for_text('this is matching')
    target_annotations = [
      TargetAnnotation('this is. matching', TAG1)
    ]
    doc = _document_for_tokens([matching_tokens])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_annotate_ignoring_space_after_dot_short_sequence(self):
    matching_tokens = [
      SimpleToken('A.B.,')
    ]
    target_annotations = [
      TargetAnnotation('A. B.', TAG1)
    ]
    doc = _document_for_tokens([matching_tokens])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_annotate_ignoring_comma_after_short_sequence(self):
    matching_tokens = [
      SimpleToken('Name,'),
    ]
    target_annotations = [
      TargetAnnotation('Name', TAG1)
    ]
    doc = _document_for_tokens([matching_tokens])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_annotate_with_local_matching_smaller_gaps(self):
    matching_tokens = _tokens_for_text('this is matching')
    target_annotations = [
      TargetAnnotation('this is. matching indeed matching', TAG1)
    ]
    # this should align with 'this is_ matching' with one gap'
    # instead of globally 'this is_ ________ ______ matching'
    # (which would result in a worse b_gap_ratio)
    doc = _document_for_tokens([matching_tokens])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_not_annotate_fuzzily_matching_with_many_differences(self):
    matching_tokens = _tokens_for_text('this is matching')
    target_annotations = [
      TargetAnnotation('txhxixsx ixsx mxaxtxcxhxixnxgx', TAG1)
    ]
    doc = _document_for_tokens([matching_tokens])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [None] * len(matching_tokens)

  def test_should_annotate_fuzzily_matching_longer_matches_based_on_ratio(self):
    long_matching_text = 'this is matching and is really really long match that we can trust'
    matching_tokens = _tokens_for_text(long_matching_text)
    no_matching_tokens = _tokens_for_text('what comes next is different')
    target_annotations = [
      TargetAnnotation(long_matching_text + ' but this is not and is another matter', TAG1)
    ]
    doc = _document_for_tokens([
      matching_tokens + no_matching_tokens
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(no_matching_tokens) == [None] * len(no_matching_tokens)

  def test_should_not_annotate_not_matching(self):
    not_matching_tokens = _tokens_for_text('something completely different')
    target_annotations = [
      TargetAnnotation('this is matching', TAG1)
    ]
    doc = _document_for_tokens([not_matching_tokens])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(not_matching_tokens) == [None] * len(not_matching_tokens)

  def test_should_annotate_exactly_matching_across_multiple_lines(self):
    matching_tokens_per_line = [
      _tokens_for_text('this is matching'),
      _tokens_for_text('and continues here')
    ]
    matching_tokens = flatten(matching_tokens_per_line)
    target_annotations = [
      TargetAnnotation('this is matching and continues here', TAG1)
    ]
    doc = _document_for_tokens(matching_tokens_per_line)
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_annotate_over_multiple_lines_with_tag_transition(self):
    tag1_tokens_by_line = [
      _tokens_for_text('this may'),
      _tokens_for_text('match')
    ]
    tag1_tokens = flatten(tag1_tokens_by_line)
    tag2_tokens_by_line = [
      _tokens_for_text('another'),
      _tokens_for_text('tag here')
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
    doc = _document_for_tokens(tokens_by_line)
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(tag1_tokens) == [TAG1] * len(tag1_tokens)
    assert _get_tags_of_tokens(tag2_tokens) == [TAG2] * len(tag2_tokens)

  def test_should_not_annotate_too_short_match_of_longer_sequence(self):
    matching_tokens = _tokens_for_text('this is matching')
    too_short_tokens = _tokens_for_text('1')
    tokens_per_line = [
      too_short_tokens,
      matching_tokens
    ]
    target_annotations = [
      TargetAnnotation('this is matching 1', TAG1)
    ]
    doc = _document_for_tokens(tokens_per_line)
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(too_short_tokens) == [None] * len(too_short_tokens)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_not_annotate_similar_sequence_multiple_times(self):
    matching_tokens_per_line = [
      _tokens_for_text('this is matching'),
      _tokens_for_text('and continues here')
    ]
    not_matching_tokens = _tokens_for_text('this is matching')

    matching_tokens = flatten(matching_tokens_per_line)
    target_annotations = [
      TargetAnnotation('this is matching and continues here', TAG1)
    ]
    doc = _document_for_tokens(
      matching_tokens_per_line + [not_matching_tokens]
    )
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(not_matching_tokens) == [None] * len(not_matching_tokens)

  def test_should_annotate_same_sequence_multiple_times_if_enabled(self):
    matching_tokens_per_line = [
      _tokens_for_text('this is matching'),
      _tokens_for_text('this is matching')
    ]

    matching_tokens = flatten(matching_tokens_per_line)
    target_annotations = [
      TargetAnnotation('this is matching', TAG1, match_multiple=True)
    ]
    doc = _document_for_tokens(matching_tokens_per_line)
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_not_override_annotation(self):
    matching_tokens_per_line = [
      _tokens_for_text('this is matching')
    ]

    matching_tokens = flatten(matching_tokens_per_line)
    target_annotations = [
      TargetAnnotation('this is matching', TAG1),
      TargetAnnotation('matching', TAG2)
    ]
    doc = _document_for_tokens(matching_tokens_per_line)
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_not_annotate_pre_annotated_tokens_on_separate_lines(self):
    line_no_tokens = _tokens_for_text('1')
    line_no_tokens[0].set_tag('line_no')
    matching_tokens = _tokens_for_text('this is matching')
    target_annotations = [
      TargetAnnotation('1', TAG2),
      TargetAnnotation('this is matching', TAG1)
    ]
    doc = _document_for_tokens([
      line_no_tokens + matching_tokens
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(line_no_tokens) == ['line_no'] * len(line_no_tokens)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)

  def test_should_annotate_shorter_target_annotation_in_longer_line(self):
    pre_tokens = _tokens_for_text('pre')
    matching_tokens = _tokens_for_text('this is matching')
    post_tokens = _tokens_for_text('post')
    target_annotations = [
      TargetAnnotation('this is matching', TAG1)
    ]
    doc = _document_for_tokens([
      pre_tokens + matching_tokens + post_tokens
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(pre_tokens) == [None] * len(pre_tokens)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(post_tokens) == [None] * len(post_tokens)

  def test_should_annotate_shorter_target_annotation_fuzzily(self):
    pre_tokens = _tokens_for_text('pre')
    matching_tokens = _tokens_for_text('this is matching')
    post_tokens = _tokens_for_text('post')
    target_annotations = [
      TargetAnnotation('this is. matching', TAG1)
    ]
    doc = _document_for_tokens([
      pre_tokens + matching_tokens + post_tokens
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(pre_tokens) == [None] * len(pre_tokens)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(post_tokens) == [None] * len(post_tokens)

  def test_should_annotate_multiple_shorter_target_annotation_in_longer_line(self):
    pre_tokens = _tokens_for_text('pre')
    matching_tokens_tag_1 = _tokens_for_text('this is matching')
    mid_tokens = _tokens_for_text('mid')
    matching_tokens_tag_2 = _tokens_for_text('also good')
    post_tokens = _tokens_for_text('post')
    target_annotations = [
      TargetAnnotation('this is matching', TAG1),
      TargetAnnotation('also good', TAG2)
    ]
    doc = _document_for_tokens([
      pre_tokens + matching_tokens_tag_1 + mid_tokens + matching_tokens_tag_2 + post_tokens
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(pre_tokens) == [None] * len(pre_tokens)
    assert _get_tags_of_tokens(matching_tokens_tag_1) == [TAG1] * len(matching_tokens_tag_1)
    assert _get_tags_of_tokens(mid_tokens) == [None] * len(mid_tokens)
    assert _get_tags_of_tokens(matching_tokens_tag_2) == [TAG2] * len(matching_tokens_tag_2)
    assert _get_tags_of_tokens(post_tokens) == [None] * len(post_tokens)

  def test_should_not_annotate_shorter_target_annotation_in_longer_line_multiple_times(self):
    pre_tokens = _tokens_for_text('pre')
    matching_tokens = _tokens_for_text('this is matching')
    post_tokens = _tokens_for_text('post')
    first_line_tokens = pre_tokens + matching_tokens + post_tokens
    similar_line_tokens = _copy_tokens(first_line_tokens)
    target_annotations = [
      TargetAnnotation('this is matching', TAG1)
    ]
    doc = _document_for_tokens([
      first_line_tokens,
      similar_line_tokens
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(similar_line_tokens) == [None] * len(similar_line_tokens)

  def test_should_annotate_shorter_target_annotation_in_longer_line_multiple_times_if_enabled(self):
    pre_tokens = _tokens_for_text('pre')
    matching_tokens = _tokens_for_text('this is matching')
    post_tokens = _tokens_for_text('post')
    same_matching_tokens = _copy_tokens(matching_tokens)
    target_annotations = [
      TargetAnnotation('this is matching', TAG1, match_multiple=True)
    ]
    doc = _document_for_tokens([
      pre_tokens + matching_tokens + post_tokens,
      _copy_tokens(pre_tokens) + same_matching_tokens + _copy_tokens(post_tokens)
    ])
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(same_matching_tokens) == [TAG1] * len(same_matching_tokens)
