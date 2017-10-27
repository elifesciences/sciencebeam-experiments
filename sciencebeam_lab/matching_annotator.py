import logging

from sciencebeam_lab.annotator import (
  AbstractAnnotator
)

def get_logger():
  return logging.getLogger(__name__)

class TargetAnnotation(object):
  def __init__(self, value, name):
    self.value = value
    self.name = name

  def __str__(self):
    return '{}: {}'.format(self.name, self.value)

class SequenceWrapper(object):
  def __init__(self, structured_document, tokens):
    self.tokens = tokens
    self.tokens_as_str = ' '.join([structured_document.get_text(t) for t in tokens])

  def __str__(self):
    return self.tokens_as_str

class SequenceMatch(object):
  def __init__(self, sequence, choice, index_range):
    self.sequence = sequence
    self.choice = choice
    self.index_range = index_range

def find_best_matches(sequence, choices):
  start_index = 0
  s1 = str(sequence)
  for choice in choices:
    choice_str = str(choice)
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
