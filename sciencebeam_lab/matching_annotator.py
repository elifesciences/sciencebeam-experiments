from __future__ import division

import logging
import re
import json
from configparser import ConfigParser
from builtins import str as text
from itertools import tee, chain

from future.utils import python_2_unicode_compatible

from sciencebeam_lab.alignment.align import (
  LocalSequenceMatcher,
  SimpleScoring
)
from sciencebeam_lab.alignment.WordSequenceMatcher import (
  WordSequenceMatcher
)

from sciencebeam_lab.collection_utils import (
  filter_truthy,
  strip_all,
  iter_flatten
)

from sciencebeam_lab.xml_utils import (
  get_text_content,
  get_text_content_list
)

from sciencebeam_lab.annotator import (
  AbstractAnnotator
)

THIN_SPACE = u'\u2009'
EN_DASH = u'\u2013'
EM_DASH = u'\u2014'

DEFAULT_SCORING = SimpleScoring(
  match_score=2,
  mismatch_score=-1,
  gap_score=-2
)

DEFAULT_SCORE_THRESHOLD = 0.9
DEFAULT_MAX_MATCH_GAP = 5

def get_logger():
  return logging.getLogger(__name__)

def normalise_str(s):
  return s.lower().replace(EM_DASH, u'-').replace(EN_DASH, u'-').replace(THIN_SPACE, ' ')

def normalise_str_or_list(x):
  if isinstance(x, list):
    return [normalise_str(s) for s in x]
  else:
    return normalise_str(x)

class XmlMapping(object):
  REGEX_SUFFIX = '.regex'
  MATCH_MULTIPLE = '.match-multiple'
  BONDING = '.bonding'
  CHILDREN = '.children'
  CHILDREN_CONCAT = '.children.concat'

@python_2_unicode_compatible
class TargetAnnotation(object):
  def __init__(self, value, name, match_multiple=False, bonding=False):
    self.value = value
    self.name = name
    self.match_multiple = match_multiple
    self.bonding = bonding

  def __str__(self):
    return u'{} (match_multiple={}): {}'.format(self.name, self.match_multiple, self.value)

class SequenceWrapper(object):
  def __init__(self, structured_document, tokens, str_filter_f=None):
    self.structured_document = structured_document
    self.str_filter_f = str_filter_f
    self.tokens = tokens
    self.token_str_list = [structured_document.get_text(t) or '' for t in tokens]
    self.tokens_as_str = ' '.join(self.token_str_list)
    if str_filter_f:
      self.tokens_as_str = str_filter_f(self.tokens_as_str)

  def tokens_between(self, index_range):
    start, end = index_range
    i = 0
    for token, token_str in zip(self.tokens, self.token_str_list):
      if i >= end:
        break
      token_end = i + len(token_str)
      if token_end > start:
        yield token
      i = token_end + 1

  def sub_sequence_for_tokens(self, tokens):
    return SequenceWrapper(self.structured_document, tokens, str_filter_f=self.str_filter_f)

  def untagged_sub_sequences(self):
    token_tags = [self.structured_document.get_tag(t) for t in self.tokens]
    tagged_count = len([t for t in token_tags if t])
    if tagged_count == 0:
      yield self
    elif tagged_count == len(self.tokens):
      pass
    else:
      untagged_tokens = []
      for token, tag in zip(self.tokens, token_tags):
        if not tag:
          untagged_tokens.append(token)
        elif untagged_tokens:
          yield self.sub_sequence_for_tokens(untagged_tokens)
          untagged_tokens = []
      if untagged_tokens:
        yield self.sub_sequence_for_tokens(untagged_tokens)

  def __str__(self):
    return self.tokens_as_str

class SequenceWrapperWithPosition(SequenceWrapper):
  def __init__(self, *args, position=None, **kwargs):
    super(SequenceWrapperWithPosition, self).__init__(*args, **kwargs)
    self.position = position

  def sub_sequence_for_tokens(self, tokens):
    return SequenceWrapperWithPosition(
      self.structured_document, tokens,
      str_filter_f=self.str_filter_f,
      position=self.position
    )

@python_2_unicode_compatible
class SequenceMatch(object):
  def __init__(self, seq1, seq2, index1_range, index2_range):
    self.seq1 = seq1
    self.seq2 = seq2
    self.index1_range = index1_range
    self.index2_range = index2_range

  def __str__(self):
    return u"SequenceMatch('{}'[{}:{}], '{}'[{}:{}])".format(
      self.seq1,
      self.index1_range[0],
      self.index1_range[1],
      self.seq2,
      self.index2_range[0],
      self.index2_range[1]
    )

@python_2_unicode_compatible
class LazyStr(object):
  def __init__(self, fn):
    self.fn = fn

  def __str__(self):
    return self.fn()

def len_index_range(index_range):
  return index_range[1] - index_range[0]

# Treat space or comma after a dot as junk
DEFAULT_ISJUNK = lambda s, i: i > 0 and s[i - 1] == '.' and (s[i] == ' ' or s[i] == ',')

class FuzzyMatchResult(object):
  def __init__(self, a, b, matching_blocks):
    self.a = a
    self.b = b
    self.matching_blocks = matching_blocks
    self.non_empty_matching_blocks = [x for x in self.matching_blocks if x[-1]]
    self._match_count = None
    self._a_index_range = None
    self._b_index_range = None
    self.isjunk = DEFAULT_ISJUNK

  def match_count(self):
    if self._match_count is None:
      self._match_count = sum(triple[-1] for triple in self.matching_blocks)
    return self._match_count

  def ratio_to(self, size):
    if not size:
      return 0.0
    return self.match_count() / size

  def ratio(self):
    a_match_len = len_index_range(self.a_index_range())
    b_match_len = len_index_range(self.b_index_range())
    max_len = max(a_match_len, b_match_len)
    if max_len == a_match_len:
      junk_match_count = self.a_junk_match_count()
    else:
      junk_match_count = self.b_junk_match_count()
    max_len_excl_junk = max_len - junk_match_count
    return self.ratio_to(max_len_excl_junk)

  def count_junk_between(self, s, index_range):
    if not self.isjunk:
      return 0
    return sum(self.isjunk(s, i) for i in range(index_range[0], index_range[1]))

  def a_junk_match_count(self):
    return self.count_junk_between(self.a, self.a_index_range())

  def b_junk_match_count(self):
    return self.count_junk_between(self.b, self.b_index_range())

  def b_junk_count(self):
    return self.count_junk_between(self.b, (0, len(self.b)))

  def a_ratio(self):
    return self.ratio_to(len(self.a) - self.a_junk_match_count())

  def b_ratio(self):
    return self.ratio_to(len(self.b) - self.b_junk_match_count())

  def b_gap_ratio(self):
    """
    Calculates the ratio of matches vs the length of b,
    but also adds any gaps / mismatches within a.
    """
    a_index_range = self.a_index_range()
    a_match_len = len_index_range(a_index_range)
    match_count = self.match_count()
    a_junk_match_count = self.a_junk_match_count()
    b_junk_count = self.b_junk_count()
    a_gaps = a_match_len - match_count
    return match_count / (len(self.b) + a_gaps - a_junk_match_count - b_junk_count)

  def a_matching_blocks(self):
    return ((a, size) for a, _, size in self.non_empty_matching_blocks)

  def b_matching_blocks(self):
    return ((b, size) for _, b, size in self.non_empty_matching_blocks)

  def index_range_for_matching_blocks(self, seq_matching_blocks):
    list_1, list_2 = tee(seq_matching_blocks)
    return (
      min(i for i, size in list_1),
      max(i + size for i, size in list_2)
    )

  def a_index_range(self):
    if not self.non_empty_matching_blocks:
      return (0, 0)
    if not self._a_index_range:
      self._a_index_range = self.index_range_for_matching_blocks(self.a_matching_blocks())
    return self._a_index_range

  def b_index_range(self):
    if not self.non_empty_matching_blocks:
      return (0, 0)
    if not self._b_index_range:
      self._b_index_range = self.index_range_for_matching_blocks(self.b_matching_blocks())
    return self._b_index_range

  def detailed_str(self):
    return 'matching_blocks=[%s]' % (
      ', '.join([
        '(a[%d:+%d] = b[%d:+%d] = "%s")' % (ai, size, bi, size, self.a[ai:ai + size])
        for ai, bi, size in self.non_empty_matching_blocks
      ])
    )

  def detailed(self):
    return LazyStr(self.detailed_str)

  def __str__(self):
    return (
      'FuzzyMatchResult(matching_blocks={}, match_count={}, ratio={},'
      ' a_ratio={}, b_gap_ratio={})'.format(
        self.matching_blocks, self.match_count(), self.ratio(), self.a_ratio(), self.b_gap_ratio()
      )
    )

def fuzzy_match(a, b, exact_word_match_threshold=5):
  if min(len(a), len(b)) < exact_word_match_threshold:
    sm = WordSequenceMatcher(None, a, b)
  else:
    sm = LocalSequenceMatcher(a=a, b=b, scoring=DEFAULT_SCORING)
  matching_blocks = sm.get_matching_blocks()
  return FuzzyMatchResult(a, b, matching_blocks)

@python_2_unicode_compatible
class PositionedSequenceSet(object):
  def __init__(self):
    self.data = set()

  def add(self, sequence):
    self.data.add(sequence.position)

  def is_close_to_any(self, sequence, max_gap):
    if not max_gap or not self.data:
      return True
    position = sequence.position
    max_distance = max_gap + 1
    for other_position in self.data:
      if abs(position - other_position) <= max_distance:
        return True
    return False

  def __str__(self):
    return str(self.data)

def offset_range_by(index_range, offset):
  if not offset:
    return index_range
  return (offset + index_range[0], offset + index_range[1])

def skip_whitespaces(s, start):
  while start < len(s) and s[start].isspace():
    start += 1
  return start

def get_fuzzy_match_filter(
  b_score_threshold, min_match_count, total_match_threshold,
  ratio_min_match_count, ratio_threshold):
  def check(m):
    if (
      m.match_count() >= ratio_min_match_count and
      m.ratio() >= ratio_threshold):
      return True
    return (
      m.b_gap_ratio() >= b_score_threshold and
      (
        m.match_count() >= min_match_count or
        m.a_ratio() >= total_match_threshold
      )
    )
  return check

DEFAULT_SEQ_FUZZY_MATCH_FILTER = get_fuzzy_match_filter(
  DEFAULT_SCORE_THRESHOLD,
  5,
  0.9,
  50,
  0.9
)

DEFAULT_CHOICE_FUZZY_MATCH_FILTER = get_fuzzy_match_filter(
  DEFAULT_SCORE_THRESHOLD,
  1,
  0.9,
  100,
  0.9
)

def find_best_matches(
  target_annotation,
  sequence,
  choices,
  seq_match_filter=DEFAULT_SEQ_FUZZY_MATCH_FILTER,
  choice_match_filter=DEFAULT_CHOICE_FUZZY_MATCH_FILTER,
  max_gap=DEFAULT_MAX_MATCH_GAP,
  matched_choices=None):

  if matched_choices is None:
    matched_choices = PositionedSequenceSet()
  if isinstance(sequence, list):
    get_logger().debug('found sequence list: %s', sequence)
    # Use tee as choices may be an iterable instead of a list
    for s, sub_choices in zip(sequence, tee(choices, len(sequence))):
      matches = find_best_matches(
        target_annotation,
        s,
        sub_choices,
        seq_match_filter=seq_match_filter,
        choice_match_filter=choice_match_filter,
        max_gap=max_gap,
        matched_choices=matched_choices
      )
      for m in matches:
        yield m
    return
  start_index = 0
  s1 = text(sequence)
  too_distant_choices = []
  for choice in choices:
    choice_str = text(choice)
    if not choice_str:
      return
    if not matched_choices.is_close_to_any(choice, max_gap=max_gap):
      too_distant_choices.append(choice)
      continue
    if len(s1) - start_index >= len(choice_str):
      m = fuzzy_match(s1, choice_str)
      get_logger().debug('choice: s1=%s, choice=%s, m=%s', s1, choice, m)
      get_logger().debug('detailed match: %s', m.detailed())
      if seq_match_filter(m):
        index1_range = m.a_index_range()
        index2_range = m.b_index_range()
        index1_end = index1_range[1]
        m = SequenceMatch(
          sequence,
          choice,
          index1_range,
          index2_range
        )
        matched_choices.add(choice)
        get_logger().debug('found match: %s', m)
        yield m
        index1_end = skip_whitespaces(s1, index1_end)
        if index1_end >= len(s1):
          get_logger().debug('end reached: %d >= %d', index1_end, len(s1))
          if target_annotation.match_multiple:
            start_index = 0
          else:
            break
        else:
          start_index = index1_end
          get_logger().debug('setting start index to: %d', start_index)
    else:
      s1_sub = s1[start_index:]
      m = fuzzy_match(choice_str, s1_sub)
      get_logger().debug('choice: s1_sub=%s, choice=%s, m=%s (in right)', s1_sub, choice, m)
      get_logger().debug('detailed match: %s', m.detailed())
      if choice_match_filter(m):
        index1_range = offset_range_by(m.b_index_range(), start_index)
        index2_range = m.a_index_range()
        get_logger().debug('index2_range: %s, start_index: %d', index2_range, start_index)
        m = SequenceMatch(
          sequence,
          choice,
          index1_range,
          index2_range
        )
        matched_choices.add(choice)
        get_logger().debug('found match: %s', m)
        yield m
        if not target_annotation.match_multiple:
          break
  if too_distant_choices:
    get_logger().debug(
      'ignored too distant choices: matched=%s (ignored=%s)',
      matched_choices,
      LazyStr(lambda: ' '.join(str(choice.position) for choice in too_distant_choices))
    )

def parse_xml_mapping(xml_mapping_filename):
  with open(xml_mapping_filename, 'r') as f:
    config = ConfigParser()
    config.read_file(f)
    return config

def apply_pattern(s, compiled_pattern):
  m = compiled_pattern.match(s)
  if m:
    get_logger().debug('regex match: %s -> %s', compiled_pattern, m.groups())
    return m.group(1)
  return s

def iter_parents(children):
  for child in children:
    p = child.getparent()
    if p:
      yield p

def exclude_parents(children):
  if not isinstance(children, list):
    children = list(children)
  all_parents = set(iter_parents(children))
  return [child for child in children if not child in all_parents]

def parse_children_concat(parent, children_concat):
  used_nodes = set()
  concat_values_list = []
  get_logger().debug('children_concat: %s', children_concat)
  for children_concat_item in children_concat:
    temp_used_nodes = set()
    temp_concat_values = []
    for children_concat_source in children_concat_item:
      xpath = children_concat_source.get('xpath')
      if xpath:
        matching_nodes = parent.xpath(xpath)
        if not matching_nodes:
          get_logger().info('no item found for, skipping concat: %s', xpath)
          temp_used_nodes = set()
          temp_concat_values = []
          break
        temp_used_nodes |= set(matching_nodes)
        value = ' '.join(get_text_content_list(exclude_parents(matching_nodes)))
      else:
        value = children_concat_source.get('value')
      temp_concat_values.append(value or '')
    used_nodes |= temp_used_nodes
    if temp_concat_values:
      concat_values_list.append(''.join(temp_concat_values))
  return concat_values_list, used_nodes

def parse_children(parent, children_pattern, children_concat):
  concat_values_list, used_nodes = parse_children_concat(parent, children_concat)
  text_content_list = filter_truthy(strip_all(
    get_text_content_list(exclude_parents(
      node for node in parent.xpath(children_pattern) if not node in used_nodes
    )) + concat_values_list
  ))
  return text_content_list


def xml_root_to_target_annotations(xml_root, xml_mapping):
  if not xml_root.tag in xml_mapping:
    raise Exception("unrecognised tag: {} (available: {})".format(
      xml_root.tag, xml_mapping.sections())
    )

  mapping = xml_mapping[xml_root.tag]

  field_names = [k for k in mapping.keys() if '.' not in k]
  get_mapping_flag = lambda k, suffix: mapping.get(k + suffix) == 'true'
  get_match_multiple = lambda k: get_mapping_flag(k, XmlMapping.MATCH_MULTIPLE)
  get_bonding_flag = lambda k: get_mapping_flag(k, XmlMapping.BONDING)

  get_logger().debug('fields: %s', field_names)

  first_page = get_text_content_list(xml_root.xpath('front/article-meta/fpage'))
  last_page = get_text_content_list(xml_root.xpath('front/article-meta/lpage'))
  keywords = get_text_content_list(
    xml_root.xpath('front/article-meta/kwd-group/kwd[@kwd-group-type="author-keywords"]')
  )
  if keywords:
    keywords = [' '.join(keywords)]
  if len(first_page) == 1 and len(last_page) == 1:
    pages = [str(p) for p in range(int(first_page[0]), int(last_page[0]) + 1)]
  else:
    pages = []
  target_annotations = []
  target_annotations_with_pos = []
  xml_pos_by_node = {node: i for i, node in enumerate(xml_root.iter())}
  for k in field_names:
    match_multiple = get_match_multiple(k)
    bonding = get_bonding_flag(k)
    children_pattern = mapping.get(k + XmlMapping.CHILDREN)
    children_concat_str = mapping.get(k + XmlMapping.CHILDREN_CONCAT)
    children_concat = json.loads(children_concat_str) if children_concat_str else []
    re_pattern = mapping.get(k + XmlMapping.REGEX_SUFFIX)
    re_compiled_pattern = re.compile(re_pattern) if re_pattern else None

    xpaths = strip_all(mapping[k].strip().split('\n'))
    get_logger().debug('xpaths(%s): %s', k, xpaths)
    for e in chain(*[xml_root.xpath(s) for s in xpaths]):
      e_pos = xml_pos_by_node.get(e)
      if children_pattern:
        text_content_list = parse_children(
          e, children_pattern, children_concat
        )
      else:
        text_content_list = filter_truthy(strip_all([get_text_content(e)]))
      if re_compiled_pattern:
        text_content_list = filter_truthy([
          apply_pattern(s, re_compiled_pattern) for s in text_content_list
        ])
      if text_content_list:
        value = (
          text_content_list[0]
          if len(text_content_list) == 1
          else sorted(text_content_list, key=lambda s: -len(s))
        )
        target_annotations_with_pos.append((
          e_pos,
          TargetAnnotation(
            value,
            k,
            match_multiple=match_multiple,
            bonding=bonding
          )
        ))
  target_annotations_with_pos = sorted(
    target_annotations_with_pos,
    key=lambda x: x[0]
  )
  target_annotations.extend(
    x[1] for x in target_annotations_with_pos
  )
  create_builtin_target_annotation = lambda s, k: TargetAnnotation(
    s, k, match_multiple=get_match_multiple(k)
  )
  target_annotations = (
    target_annotations +
    [create_builtin_target_annotation(s, 'keywords') for s in keywords] +
    [create_builtin_target_annotation(s, 'page_no') for s in pages]
  )
  get_logger().debug('target_annotations:\n%s', '\n'.join([
    ' ' + str(a) for a in target_annotations
  ]))
  return target_annotations

class MatchingAnnotator(AbstractAnnotator):
  def __init__(self, target_annotations):
    self.target_annotations = target_annotations

  def annotate(self, structured_document):
    pending_sequences = []
    for page in structured_document.get_pages():
      for line in structured_document.get_lines_of_page(page):
        tokens = [
          token
          for token in structured_document.get_tokens_of_line(line)
          if not structured_document.get_tag(token)
        ]
        if tokens:
          get_logger().debug(
            'tokens without tag: %s',
            [structured_document.get_text(token) for token in tokens]
          )
          pending_sequences.append(SequenceWrapperWithPosition(
            structured_document,
            tokens,
            normalise_str,
            position=len(pending_sequences)
          ))

    matched_choices_map = dict()
    for target_annotation in self.target_annotations:
      get_logger().debug('target annotation: %s', target_annotation)
      target_value = normalise_str_or_list(target_annotation.value)
      untagged_pending_sequences = iter_flatten(
        seq.untagged_sub_sequences() for seq in pending_sequences
      )
      if target_annotation.bonding:
        matched_choices = matched_choices_map.setdefault(
          target_annotation.name,
          PositionedSequenceSet()
        )
      else:
        matched_choices = PositionedSequenceSet()
      matches = find_best_matches(
        target_annotation,
        target_value,
        untagged_pending_sequences,
        matched_choices=matched_choices
      )
      for m in matches:
        choice = m.seq2
        matching_tokens = list(choice.tokens_between(m.index2_range))
        get_logger().debug(
          'matching_tokens: %s %s',
          [structured_document.get_text(token) for token in matching_tokens],
          m.index2_range
        )
        for token in matching_tokens:
          if not structured_document.get_tag(token):
            structured_document.set_tag(
              token,
              target_annotation.name
            )
    return structured_document
