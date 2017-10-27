import logging
from configparser import ConfigParser
from builtins import str as text

from future.utils import python_2_unicode_compatible

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
    self.tokens_as_str = ' '.join([structured_document.get_text(t) or '' for t in tokens])

  def __str__(self):
    return self.tokens_as_str

class SequenceMatch(object):
  def __init__(self, sequence, choice, index_range):
    self.sequence = sequence
    self.choice = choice
    self.index_range = index_range

def find_best_matches(sequence, choices):
  start_index = 0
  s1 = text(sequence)
  for choice in choices:
    choice_str = text(choice)
    i = s1.find(choice_str, start_index)
    get_logger().debug('choice: %s - %s - %d', s1, choice, i)
    if i >= 0:
      end_index = i + len(choice_str)
      get_logger().debug('found match: %s - %d:%d', s1, i, end_index)
      yield SequenceMatch(sequence, choice, (i, end_index))
      if end_index >= len(s1):
        get_logger().debug('end reached: %d >= %d', end_index, len(s1))
        break
      else:
        start_index = end_index
        get_logger().debug('setting start index to: %d', start_index)

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
  keywords = get_text_content_list(xml_root.xpath('front/article-meta/kwd-group/kwd'))
  if len(keywords) > 0:
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
  author_aff = (
    get_text_content_list(xml_root.xpath('front/article-meta/contrib-group/aff')) +
    get_text_content_list(xml_root.xpath('front/article-meta/contrib-group/contrib/aff')) +
    get_text_content_list(xml_root.xpath('front/article-meta/aff'))
  )
  target_annotations = []
  for k in field_names:
    for e in xml_root.xpath(mapping[k]):
      text = (get_text_content(
        e
      ) or '').strip()
      if len(text) > 0:
        target_annotations.append(
          TargetAnnotation(text, k)
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
        pending_sequences.append(SequenceWrapper(
          structured_document,
          structured_document.get_tokens_of_line(line)
        ))

    for target_annotation in self.target_annotations:
      for m in find_best_matches(target_annotation.value, pending_sequences):
        for token in m.choice.tokens:
          structured_document.set_tag(
            token,
            target_annotation.name
          )
    return structured_document
