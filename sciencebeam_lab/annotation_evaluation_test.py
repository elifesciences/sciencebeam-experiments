from __future__ import division

from sciencebeam_lab.structured_document import (
  SimpleStructuredDocument,
  SimpleLine,
  SimpleToken
)

from sciencebeam_lab.annotation_evaluation import (
  evaluate_document_by_page
)

TAG1 = 'tag1'

class TestEvaluateDocumentByPage(object):
  def test_should_return_ratio_and_count_of_tagged_tokens(self):
    tagged_tokens = [
      SimpleToken('this'),
      SimpleToken('is'),
      SimpleToken('tagged')
    ]
    not_tagged_tokens = [
      SimpleToken('this'),
      SimpleToken('isn\'t')
    ]
    doc = SimpleStructuredDocument(lines=[SimpleLine(
      tagged_tokens + not_tagged_tokens
    )])
    for token in tagged_tokens:
      doc.set_tag(token, TAG1)
    num_total = len(tagged_tokens) + len(not_tagged_tokens)
    results = evaluate_document_by_page(doc)
    assert results == [{
      'count': {
        TAG1: len(tagged_tokens),
        None: len(not_tagged_tokens)
      },
      'percentage': {
        TAG1: len(tagged_tokens) / num_total,
        None: len(not_tagged_tokens) / num_total
      }
    }]
