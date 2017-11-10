from __future__ import division

from collections import Counter
import csv

from six import iteritems

from sciencebeam_lab.collection_utils import (
  flatten
)

class EvaluationFields(object):
  DOCUMENT = 'document'
  PAGE = 'page'
  TAG = 'tag'
  COUNT = 'count'

DEFAULT_EVALUATION_COLUMNS = [
  EvaluationFields.DOCUMENT,
  EvaluationFields.PAGE,
  EvaluationFields.TAG,
  EvaluationFields.COUNT
]

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

def to_csv_dict_rows(evaluation_result, document=None):
  return flatten(
    [
      {
        EvaluationFields.DOCUMENT: document,
        EvaluationFields.PAGE: 1 + page_index,
        EvaluationFields.TAG: tag,
        EvaluationFields.COUNT: count
      }
      for tag, count in iteritems(page_evaluation['count'])
    ]
    for page_index, page_evaluation in enumerate(evaluation_result)
  )

def to_csv_file(fp, evaluation_result, fields=None, filename=None, document=None):
  if fields is None:
    fields = DEFAULT_EVALUATION_COLUMNS
  writer = csv.writer(
    fp,
    delimiter='\t' if filename and filename.endswith('.tsv') else ','
  )
  writer.writerow(fields)
  writer.writerows((
    [row.get(k) for k in fields]
    for row in to_csv_dict_rows(evaluation_result, document)
  ))
