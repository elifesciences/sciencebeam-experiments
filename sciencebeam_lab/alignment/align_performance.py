from __future__ import absolute_import, print_function

import logging
import timeit
from contextlib import contextmanager

import numpy as np

from sciencebeam_lab.alignment.align import (
  SimpleScoring,
  CustomScoring,
  LocalSequenceMatcher
)

DEFAULT_MATCH_SCORE = 2
DEFAULT_MISMATCH_SCORE = -1
DEFAULT_GAP_SCORE = -3

DEFAULT_SCORING = SimpleScoring(
  DEFAULT_MATCH_SCORE, DEFAULT_MISMATCH_SCORE, DEFAULT_GAP_SCORE
)

CUSTOM_SCORING = CustomScoring(
  lambda a, b: DEFAULT_MATCH_SCORE if a == b else DEFAULT_MISMATCH_SCORE,
  DEFAULT_GAP_SCORE
)

SHORT_STRING1 = 'abc'
SHORT_STRING2 = 'def'

LONG_STRING1 = 'abcefghijk' * 100
LONG_STRING2 = ''.join(list(reversed(LONG_STRING1)))

def encode_str(s):
  return np.array([int(ord(x)) for x in s], dtype=np.int32)

LONG_ENCODED1 = encode_str(LONG_STRING1)
LONG_ENCODED2 = encode_str(LONG_STRING2)

@contextmanager
def disabled_cython_code():
  import sciencebeam_lab.alignment.align as align
  copy_compute_inner_alignment_matrix_simple_scoring_int = (
    align.compute_inner_alignment_matrix_simple_scoring_int
  )
  copy_compute_inner_alignment_matrix_simple_scoring_any = (
    align.compute_inner_alignment_matrix_simple_scoring_any
  )
  copy_compute_inner_alignment_matrix_scoring_fn_any = (
    align.compute_inner_alignment_matrix_scoring_fn_any
  )
  align.compute_inner_alignment_matrix_simple_scoring_int = None
  align.compute_inner_alignment_matrix_simple_scoring_any = None
  align.compute_inner_alignment_matrix_scoring_fn_any = None
  yield
  align.compute_inner_alignment_matrix_simple_scoring_int = (
    copy_compute_inner_alignment_matrix_simple_scoring_int
  )
  align.compute_inner_alignment_matrix_simple_scoring_any = (
    copy_compute_inner_alignment_matrix_simple_scoring_any
  )
  align.compute_inner_alignment_matrix_scoring_fn_any = (
    copy_compute_inner_alignment_matrix_scoring_fn_any
  )
  

def test_align_with_scoring_fn_py():
  with disabled_cython_code():
    LocalSequenceMatcher(LONG_STRING1, LONG_STRING2, CUSTOM_SCORING).get_matching_blocks()

def test_align_with_scoring_fn():
  LocalSequenceMatcher(LONG_STRING1, LONG_STRING2, CUSTOM_SCORING).get_matching_blocks()

def test_align_with_simple_scoring():
  LocalSequenceMatcher(LONG_STRING1, LONG_STRING2, DEFAULT_SCORING).get_matching_blocks()

def test_align_with_simple_scoring_int():
  LocalSequenceMatcher(LONG_ENCODED1, LONG_ENCODED2, DEFAULT_SCORING).get_matching_blocks()

def test_align_with_simple_scoring_str():
  LocalSequenceMatcher(LONG_STRING1, LONG_STRING2, DEFAULT_SCORING).get_matching_blocks()

def report_timing(fn, number=1):
  timeit_result_ms = timeit.timeit(
    fn + "()",
    setup="from __main__ import " + fn,
    number=number
  ) * 1000
  print("{} ({}x):\n{:f} ms / it ({:f} ms total)\n".format(
    fn,
    number,
    timeit_result_ms / number,
    timeit_result_ms
  ))

def main():
  print("len LONG_STRING1: {}\n".format(len(LONG_STRING1)))
  print("len LONG_ENCODED1: {}\n".format(len(LONG_ENCODED1)))
  report_timing("test_align_with_scoring_fn_py")
  report_timing("test_align_with_scoring_fn", 3)
  report_timing("test_align_with_simple_scoring", 3)
  report_timing("test_align_with_simple_scoring_int", 3)
  report_timing("test_align_with_simple_scoring_str", 3)

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
