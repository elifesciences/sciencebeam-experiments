from sciencebeam_lab.find_line_number import (
  find_line_number_tokens
)

class LineAnnotator(object):
  def annotate(self, structured_document, tag='line_no'):
    for t in find_line_number_tokens(structured_document):
      structured_document.set_tag(t, tag)
    return structured_document

DEFAULT_ANNOTATORS = [
  LineAnnotator()
]

class Annotator(object):
  def __init__(self, annotators=None):
    if annotators is None:
      annotators = DEFAULT_ANNOTATORS
    self.annotators = annotators

  def annotate(self, structured_document):
    for annotator in self.annotators:
      structured_document = annotator.annotate(structured_document)
    return structured_document
