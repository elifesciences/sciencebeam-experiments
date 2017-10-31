import logging
from configparser import ConfigParser
from builtins import str as text

from future.utils import python_2_unicode_compatible

from sciencebeam_lab.utils.SequenceMatcher import (
  SequenceMatcher
)
from sciencebeam_lab.utils.WordSequenceMatcher import (
  WordSequenceMatcher
)

from sciencebeam_lab.collection_utils import (
  flatten
)

from sciencebeam_lab.xml_utils import (
  get_text_content,
  get_text_content_list
)

from sciencebeam_lab.annotator import (
  AbstractAnnotator
)

def get_logger():
  return logging.getLogger(__name__)

@python_2_unicode_compatible
class TargetAnnotation(object):
  def __init__(self, value, name):
    self.value = value
    self.name = name

  def __str__(self):
    return u'{}: {}'.format(self.name, self.value)

class SequenceWrapper(object):
  def __init__(self, structured_document, tokens):
    self.tokens = tokens
    self.token_str_list = [structured_document.get_text(t) or '' for t in tokens]
    self.tokens_as_str = ' '.join(self.token_str_list)

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

  def __str__(self):
    return self.tokens_as_str

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

class FuzzyMatchResult(object):
  def __init__(self, a, b, matching_blocks):
    self.a = a
    self.b = b
    self.matching_blocks = matching_blocks
    self.non_empty_matching_blocks = [x for x in self.matching_blocks if x[-1]]
    self._a_index_range = None
    self._b_index_range = None

  def match_count(self):
    return sum(triple[-1] for triple in self.matching_blocks)

  def a_ratio(self):
    return self.match_count() / len(self.a)

  def b_ratio(self):
    return self.match_count() / len(self.b)

  def b_gap_ratio(self):
    """
    Calculates the ratio of matches vs the length of b,
    but also adds any gaps / mismatches within a.
    """
    a_index_range = self.a_index_range()
    a_match_len = a_index_range[1] - a_index_range[0]
    match_count = self.match_count()
    a_gaps = a_match_len - match_count
    return match_count / (len(self.b) + a_gaps)

  def a_index_range(self):
    if not self.non_empty_matching_blocks:
      return (0, 0)
    if not self._a_index_range:
      self._a_index_range = (
        min(a for a, _, size in self.non_empty_matching_blocks),
        max(a + size for a, _, size in self.non_empty_matching_blocks)
      )
    return self._a_index_range

  def b_index_range(self):
    if not self.non_empty_matching_blocks:
      return (0, 0)
    if not self._b_index_range:
      self._b_index_range = (
        min(b for _, b, size in self.non_empty_matching_blocks),
        max(b + size for _, b, size in self.non_empty_matching_blocks)
      )
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
    return 'FuzzyMatchResult(matching_blocks={}, b_gap_ratio={})'.format(
      self.matching_blocks, self.b_gap_ratio()
    )

def fuzzy_match(a, b, exact_word_match_threshold=5):
  if min(len(a), len(b)) < exact_word_match_threshold:
    sm = WordSequenceMatcher(None, a, b)
  else:
    sm = SequenceMatcher(None, a, b)
  matching_blocks = sm.get_matching_blocks()
  return FuzzyMatchResult(a, b, matching_blocks)

def find_best_matches(sequence, choices, threshold=0.9):
  if isinstance(sequence, list):
    get_logger().debug('found sequence list: %s', sequence)
    for s in sequence:
      for m in find_best_matches(s, choices, threshold=threshold):
        yield m
    return
  start_index = 0
  s1 = text(sequence)
  for choice in choices:
    choice_str = text(choice)
    if not choice_str:
      return
    if len(s1) - start_index >= len(choice_str):
      m = fuzzy_match(s1, choice_str)
      get_logger().debug('choice: s1=%s, choice=%s, m=%s', s1, choice, m)
      get_logger().debug('detailed match: %s', m.detailed())
      if m.b_gap_ratio() >= threshold:
        index1_range = m.a_index_range()
        index2_range = m.b_index_range()
        index1_end = index1_range[1]
        m = SequenceMatch(
          sequence,
          choice,
          index1_range,
          index2_range
        )
        get_logger().debug('found match: %s', m)
        yield m
        if index1_end >= len(s1):
          get_logger().debug('end reached: %d >= %d', index1_end, len(s1))
          break
        else:
          start_index = index1_end
          get_logger().debug('setting start index to: %d', start_index)
    else:
      s1_sub = s1[start_index:]
      m = fuzzy_match(choice_str, s1_sub)
      get_logger().debug('choice: s1_sub=%s, choice=%s, m=%s (in right)', s1_sub, choice, m)
      get_logger().debug('detailed match: %s', m.detailed())
      if m.b_gap_ratio() >= threshold:
        index2_rel_range = m.a_index_range()
        get_logger().debug('index2_rel_range: %s, start_index: %d', index2_rel_range, start_index)
        index2_start = start_index + index2_rel_range[0]
        index2_end = start_index + index2_rel_range[1]
        m = SequenceMatch(
          sequence,
          choice,
          (start_index, start_index + len(s1_sub)),
          (index2_start, index2_end)
        )
        get_logger().debug('found match: %s', m)
        yield m

def parse_xml_mapping(xml_mapping_filename):
  with open(xml_mapping_filename, 'r') as f:
    config = ConfigParser()
    config.read_file(f)
    return config

def xml_root_to_target_annotations(xml_root, xml_mapping):
  if not xml_root.tag in xml_mapping:
    raise Exception("unrecognised tag: {} (available: {})".format(
      xml_root.tag, xml_mapping.sections())
    )

  mapping = xml_mapping[xml_root.tag]

  field_names = [k for k in mapping.keys() if '.' not in k]

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
  authors = [
    '{} {}'.format(
      get_text_content(e.find('given-names')),
      get_text_content(e.find('surname'))
    ) for e in xml_root.xpath('front/article-meta/contrib-group/contrib/name')
  ]
  author_aff_xpaths = [
    'front/article-meta/contrib-group/aff',
    'front/article-meta/contrib-group/contrib/aff',
    'front/article-meta/aff'
  ]
  author_aff = flatten([
    get_text_content_list(xml_root.xpath(xpath)) for xpath in author_aff_xpaths
  ])
  aff_extra = [
    s.strip()
    for xpath in author_aff_xpaths
    for s in get_text_content_list(
      xml_root.xpath('{}/*'.format(xpath))
    )
  ]
  get_logger().debug('aff_extra: %s', aff_extra)
  if aff_extra:
    author_aff.append(aff_extra)
  target_annotations = []
  for k in field_names:
    for e in xml_root.xpath(mapping[k]):
      text_content = (get_text_content(
        e
      ) or '').strip()
      if text_content:
        target_annotations.append(
          TargetAnnotation(text_content, k)
        )
  target_annotations = (
    target_annotations +
    [TargetAnnotation(s, 'keywords') for s in keywords] +
    [TargetAnnotation(s, 'author') for s in authors] +
    [TargetAnnotation(s, 'page_no') for s in pages] +
    [TargetAnnotation(s, 'author_aff') for s in author_aff]
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
          pending_sequences.append(SequenceWrapper(
            structured_document,
            tokens
          ))

    for target_annotation in self.target_annotations:
      get_logger().debug('target annotation: %s', target_annotation.name)
      for m in find_best_matches(target_annotation.value, pending_sequences):
        choice = m.seq2
        matching_tokens = list(choice.tokens_between(m.index2_range))
        get_logger().debug(
          'matching_tokens: %s %s',
          [structured_document.get_text(token) for token in matching_tokens],
          m.index2_range
        )
        for token in matching_tokens:
          structured_document.set_tag(
            token,
            target_annotation.name
          )
    return structured_document
