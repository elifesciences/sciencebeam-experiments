from __future__ import division

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
  FuzzyMatchResult,
  fuzzy_match,
  THIN_SPACE,
  EN_DASH,
  EM_DASH,
  XmlMappingSuffix
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

def _tokens_for_text_lines(text_lines):
  return [_tokens_for_text(line) for line in text_lines]

def _lines_for_tokens(tokens_by_line):
  return [SimpleLine(tokens) for tokens in tokens_by_line]

def _document_for_tokens(tokens_by_line):
  return SimpleStructuredDocument(lines=_lines_for_tokens(tokens_by_line))

class TestFuzzyMatch(object):
  def test_match_count_should_be_the_same_independent_of_order(self):
    s1 = 'this is a some sequence'
    choice = 'this is another sequence'
    fm_1 = fuzzy_match(s1, choice)
    fm_2 = fuzzy_match(choice, s1)
    assert fm_1.match_count() == fm_2.match_count()

class TestFuzzyMatchResult(object):
  def test_exact_match(self):
    fm = FuzzyMatchResult('abc', 'abc', [(0, 0, 3)])
    assert fm.has_match()
    assert fm.match_count() == 3
    assert fm.ratio() == 1.0
    assert fm.a_ratio() == 1.0
    assert fm.b_ratio() == 1.0
    assert fm.b_gap_ratio() == 1.0
    assert fm.a_index_range() == (0, 3)
    assert fm.b_index_range() == (0, 3)

  def test_no_match(self):
    fm = FuzzyMatchResult('abc', 'xyz', [])
    assert not fm.has_match()
    assert fm.match_count() == 0

  def test_a_split_no_match(self):
    fm = FuzzyMatchResult('abc', 'xyz', [])
    fm_1, fm_2 = fm.a_split_at(2)

    assert not fm_1.has_match()
    assert fm_1.a == 'ab'
    assert fm_1.b == 'xyz'

    assert not fm_2.has_match()
    assert fm_2.a == 'c'
    assert fm_2.b == 'xyz'

  def test_b_split_no_match(self):
    fm = FuzzyMatchResult('abc', 'xyz', [])
    fm_1, fm_2 = fm.b_split_at(2)

    assert not fm_1.has_match()
    assert fm_1.a == 'abc'
    assert fm_1.b == 'xy'

    assert not fm_2.has_match()
    assert fm_2.a == 'abc'
    assert fm_2.b == 'z'

  def test_a_split_exact_match(self):
    fm = FuzzyMatchResult('abc', 'abc', [(0, 0, 3)])
    fm_1, fm_2 = fm.a_split_at(2)

    assert fm_1.a == 'ab'
    assert fm_1.b == 'abc'
    assert fm_1.has_match()
    assert fm_1.ratio() == 1.0
    assert fm_1.a_ratio() == 1.0
    assert fm_1.b_ratio() == 2 / 3
    assert fm_1.b_gap_ratio() == 2 / 3
    assert fm_1.a_index_range() == (0, 2)
    assert fm_1.b_index_range() == (0, 2)

    assert fm_2.a == 'c'
    assert fm_2.b == 'abc'
    assert fm_2.has_match()
    assert fm_2.ratio() == 1.0
    assert fm_2.a_ratio() == 1.0
    assert fm_2.b_ratio() == 1 / 3
    assert fm_2.b_gap_ratio() == 1 / 3
    assert fm_2.a_index_range() == (0, 1)
    assert fm_2.b_index_range() == (0, 1)

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
        TAG1 + XmlMappingSuffix.REGEX: r'(?:\d+\.?)* ?(.*)'
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
        TAG1 + XmlMappingSuffix.MATCH_MULTIPLE: 'true'
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
        TAG1 + XmlMappingSuffix.BONDING: 'true'
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
        TAG1 + XmlMappingSuffix.CHILDREN: './/*'
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
        TAG1 + XmlMappingSuffix.CHILDREN: './/*'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(t.name, t.value) for t in target_annotations] == [
      (TAG1, [SOME_LONGER_VALUE, SOME_SHORTER_VALUE])
    ]

  def test_should_apply_children_xpaths_and_include_parent_text_between_matched_children(self):
    xml_root = E.article(
      E.entry(
        E.parent(
          E.child2(SOME_LONGER_VALUE),
          SOME_VALUE,
          E.child1(SOME_SHORTER_VALUE)
        )
      )
    )
    xml_mapping = {
      'article': {
        TAG1: 'entry',
        TAG1 + XmlMappingSuffix.CHILDREN: './/*'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(t.name, t.value) for t in target_annotations] == [
      (TAG1, [SOME_LONGER_VALUE, SOME_VALUE, SOME_SHORTER_VALUE])
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
        TAG1 + XmlMappingSuffix.CHILDREN: '\n{}\n{}\n'.format('.//*', '.'),
        TAG1 + XmlMappingSuffix.UNMATCHED_PARENT_TEXT: 'true'
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
        TAG1 + XmlMappingSuffix.CHILDREN: './/*',
        TAG1 + XmlMappingSuffix.CHILDREN_CONCAT: json.dumps([[{
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
        TAG1 + XmlMappingSuffix.CHILDREN: './/*',
        TAG1 + XmlMappingSuffix.CHILDREN_CONCAT: json.dumps([[{
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

  def test_should_apply_range_children(self):
    num_values = [101, 102, 103, 104, 105, 106, 107]
    xml_root = E.article(
      E.entry(
        E.child1(SOME_VALUE),
        E.fpage(str(min(num_values))),
        E.lpage(str(max(num_values)))
      )
    )
    xml_mapping = {
      'article': {
        TAG1: 'entry',
        TAG1 + XmlMappingSuffix.CHILDREN: 'fpage|lpage',
        TAG1 + XmlMappingSuffix.CHILDREN_RANGE: json.dumps([{
          'min': {
            'xpath': 'fpage'
          },
          'max': {
            'xpath': 'lpage'
          }
        }])
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(t.name, t.value) for t in target_annotations] == [
      (TAG1, [str(x) for x in num_values])
    ]

  def test_should_apply_range_children_as_separate_target_annotations(self):
    num_values = [101, 102, 103, 104, 105, 106, 107]
    xml_root = E.article(
      E.entry(
        E.child1(SOME_VALUE),
        E.fpage(str(min(num_values))),
        E.lpage(str(max(num_values)))
      )
    )
    xml_mapping = {
      'article': {
        TAG1: 'entry',
        TAG1 + XmlMappingSuffix.CHILDREN: 'fpage|lpage',
        TAG1 + XmlMappingSuffix.CHILDREN_RANGE: json.dumps([{
          'min': {
            'xpath': 'fpage'
          },
          'max': {
            'xpath': 'lpage'
          },
          'standalone': True
        }])
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(t.name, t.value) for t in target_annotations] == [
      (TAG1, str(x))
      for x in num_values
    ]

  def test_should_not_apply_range_children_if_xpath_not_matching(self):
    num_values = [101, 102, 103, 104, 105, 106, 107]
    fpage = str(min(num_values))
    lpage = str(max(num_values))
    xml_root = E.article(
      E.entry(
        E.child1(SOME_VALUE),
        E.fpage(fpage),
        E.lpage(lpage)
      )
    )
    xml_mapping = {
      'article': {
        TAG1: 'entry',
        TAG1 + XmlMappingSuffix.CHILDREN: 'fpage|unknown',
        TAG1 + XmlMappingSuffix.CHILDREN_RANGE: json.dumps([{
          'min': {
            'xpath': 'fpage'
          },
          'max': {
            'xpath': 'unknown'
          }
        }])
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(t.name, t.value) for t in target_annotations] == [
      (TAG1, fpage)
    ]

  def test_should_not_apply_range_children_if_value_is_not_integer(self):
    fpage = 'abc'
    lpage = 'xyz'
    xml_root = E.article(
      E.entry(
        E.child1(SOME_VALUE),
        E.fpage(fpage),
        E.lpage(lpage)
      )
    )
    xml_mapping = {
      'article': {
        TAG1: 'entry',
        TAG1 + XmlMappingSuffix.CHILDREN: 'fpage|lpage',
        TAG1 + XmlMappingSuffix.CHILDREN_RANGE: json.dumps([{
          'min': {
            'xpath': 'fpage'
          },
          'max': {
            'xpath': 'lpage'
          }
        }])
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(t.name, t.value) for t in target_annotations] == [
      (TAG1, [fpage, lpage])
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

  def test_should_return_target_annotations_in_order_of_priority_first(self):
    xml_root = E.article(
      E.tag1('tag1.1'), E.tag2('tag2.1'), E.tag1('tag1.2'), E.tag2('tag2.2'),
    )
    xml_mapping = {
      'article': {
        TAG1: 'tag1',
        TAG2: 'tag2',
        TAG2 + XmlMappingSuffix.PRIORITY: '1'
      }
    }
    target_annotations = xml_root_to_target_annotations(xml_root, xml_mapping)
    assert [(ta.name, ta.value) for ta in target_annotations] == [
      (TAG2, 'tag2.1'), (TAG2, 'tag2.2'), (TAG1, 'tag1.1'), (TAG1, 'tag1.2')
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

  def test_should_annotate_longer_sequence_over_multiple_lines_considering_next_line(self):
    # we need a long enough sequence to fall into the first branch
    # and match the partial match threshold
    exact_matching_text_lines = ('this may', 'indeed match very well without the slightest doubt')
    # add a short prefix that doesn't affect the score much
    # but would be skipped if we only matched the second line
    matching_text_lines = (exact_matching_text_lines[0], 'x ' + exact_matching_text_lines[1])
    matching_tokens_by_line = _tokens_for_text_lines(matching_text_lines)
    matching_tokens = flatten(matching_tokens_by_line)
    pre_tokens = _tokens_for_text(matching_text_lines[0] + ' this may not')
    post_tokens = _tokens_for_text('or not')
    tokens_by_line = [
      pre_tokens + matching_tokens_by_line[0],
      matching_tokens_by_line[1] + post_tokens
    ]
    target_annotations = [
      TargetAnnotation(' '.join(exact_matching_text_lines), TAG1)
    ]
    doc = _document_for_tokens(tokens_by_line)
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(pre_tokens) == [None] * len(pre_tokens)
    assert _get_tags_of_tokens(post_tokens) == [None] * len(post_tokens)

  def test_should_annotate_shorter_sequence_over_multiple_lines_considering_next_line(self):
    # use a short sequence that wouldn't get matched on it's own
    matching_text_lines = ('this may', 'match')
    matching_tokens_by_line = _tokens_for_text_lines(matching_text_lines)
    matching_tokens = flatten(matching_tokens_by_line)
    # repeat the same text on the two lines, only by combining the lines would it be clear
    # which tokens to match
    pre_tokens = _tokens_for_text(matching_text_lines[0] + ' be some other longer preceeding text')
    post_tokens = _tokens_for_text('this is some text after but no ' + matching_text_lines[1])
    tokens_by_line = [
      pre_tokens + matching_tokens_by_line[0],
      matching_tokens_by_line[1] + post_tokens
    ]
    target_annotations = [
      TargetAnnotation('this may match', TAG1)
    ]
    doc = _document_for_tokens(tokens_by_line)
    MatchingAnnotator(target_annotations).annotate(doc)
    assert _get_tags_of_tokens(matching_tokens) == [TAG1] * len(matching_tokens)
    assert _get_tags_of_tokens(pre_tokens) == [None] * len(pre_tokens)
    assert _get_tags_of_tokens(post_tokens) == [None] * len(post_tokens)

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
