from cpython cimport array
cimport cython

import logging

import numpy as np
cimport numpy as np

def get_logger():
  return logging.getLogger(__name__)

ctypedef np.int_t int_t
ctypedef int_t[:, :] score_matrix_t

cdef inline int imax2(int a, int b):
  if a >= b:
    return a
  else:
    return b

cdef inline int imax3(int a, int b, int c):
  if a >= b:
    return imax2(a, c)
  else:
    return imax2(b, c)

cdef inline int imax4(int a, int b, int c, int d):
  if a >= b:
    return imax3(a, c, d)
  else:
    return imax3(b, c, d)

def native_compute_inner_alignment_matrix_simple_scoring_int(
  score_matrix_t scoring_matrix,
  int[:] a,
  int[:] b,
  int match_score, int mismatch_score, int gap_score):
  cdef int m = len(a) + 1
  cdef int n = len(b) + 1
  cdef int i, j
  for i in range(1, m):
    for j in range(1, n):
      scoring_matrix[i, j] = imax4(
        0,

        # Match elements.
        scoring_matrix[i - 1, j - 1] +
        (match_score if a[i - 1] == b[j - 1] else mismatch_score),

        # Gap on sequenceA.
        scoring_matrix[i, j - 1] + gap_score,

        # Gap on sequenceB.
        scoring_matrix[i - 1, j] + gap_score
      )

def native_compute_inner_alignment_matrix_simple_scoring_any(
  score_matrix_t scoring_matrix,
  a,
  b,
  int match_score, int mismatch_score, int gap_score):
  cdef list ca = list(a)
  cdef list cb = list(b)
  cdef int m = len(ca) + 1
  cdef int n = len(cb) + 1
  cdef int i, j
  for i in range(1, m):
    for j in range(1, n):
      scoring_matrix[i, j] = imax4(
        0,

        # Match elements.
        scoring_matrix[i - 1, j - 1] +
        (match_score if ca[i - 1] == cb[j - 1] else mismatch_score),

        # Gap on sequenceA.
        scoring_matrix[i, j - 1] + gap_score,

        # Gap on sequenceB.
        scoring_matrix[i - 1, j] + gap_score
      )

def native_compute_inner_alignment_matrix_scoring_fn_any(
  score_matrix_t scoring_matrix,
  a,
  b,
  scoring_fn, int gap_score):
  cdef list ca = list(a)
  cdef list cb = list(b)
  cdef int m = len(ca) + 1
  cdef int n = len(cb) + 1
  cdef int i, j
  for i in range(1, m):
    for j in range(1, n):
      scoring_matrix[i, j] = imax4(
        0,

        # Match elements.
        scoring_matrix[i - 1, j - 1] +
        scoring_fn(ca[i - 1], cb[j - 1]),

        # Gap on sequenceA.
        scoring_matrix[i, j - 1] + gap_score,

        # Gap on sequenceB.
        scoring_matrix[i - 1, j] + gap_score
      )

cdef inline _next_loc(
  score_matrix_t score_matrix, int i, int j):
  diag_score = score_matrix[i - 1][j - 1]
  up_score = score_matrix[i - 1][j]
  left_score = score_matrix[i][j - 1]
  max_score = imax3(diag_score, up_score, left_score)
  if max_score == 0 or diag_score == 0:
    return None
  if diag_score == max_score:
    return (i - 1, j - 1)
  if up_score == max_score:
    return (i - 1, j)
  if left_score == max_score:
    return (i, j - 1)
  return None

def native_alignment_matrix_single_path_traceback(
  score_matrix_t score_matrix,
  start_loc):

  cur_loc = start_loc
  path = [cur_loc]
  while True:
    i, j = cur_loc
    next_loc = _next_loc(score_matrix, i, j)
    if not next_loc:
      return path
    else:
      cur_loc = next_loc
      path.insert(0, cur_loc)
