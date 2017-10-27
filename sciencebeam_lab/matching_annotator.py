from sciencebeam_lab.annotator import (
  AbstractAnnotator
)

class TargetAnnotation(object):
  def __init__(self, value, name):
    self.value = value
    self.name = name

  def __str__(self):
    return '{}: {}'.format(self.name, self.value)

class MatchingAnnotator(AbstractAnnotator):
  def __init__(self, target_annotations):
    self.target_annotations = target_annotations

  def annotate(self, structured_document):
    exact_map = {
      t.value: t.name
      for t in self.target_annotations
    }
    for page in structured_document.get_pages():
      for line in structured_document.get_lines_of_page(page):
        tokens = structured_document.get_tokens_of_line(line)
        matching_tag = exact_map.get(
          ' '.join([structured_document.get_text(t) for t in tokens])
        )
        if matching_tag:
          for token in tokens:
            structured_document.set_tag(
              token,
              matching_tag
            )
    return structured_document
