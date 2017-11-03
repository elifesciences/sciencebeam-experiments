from __future__ import division

from collections import Counter

from six import iteritems

def evaluate_document_page(structured_document, page):
  tag_counter = Counter()
  for line in structured_document.get_lines_of_page(page):
    tag_counter.update(
      structured_document.get_tag(token)
      for token in structured_document.get_tokens_of_line(line)
    )
  num_tokens = sum(tag_counter.values())
  return {
    'count': dict(tag_counter),
    'percentage': {
      k: c / num_tokens
      for k, c in iteritems(tag_counter)
    }
  }

def evaluate_document_by_page(structured_document):
  return [
    evaluate_document_page(structured_document, page)
    for page in structured_document.get_pages()
  ]
