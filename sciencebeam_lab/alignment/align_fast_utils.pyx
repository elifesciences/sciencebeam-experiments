# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
from cpython cimport array
cimport cython

import numpy as np
cimport numpy as np

cdef inline int imax4(int a, int b, int c, int d):
  if a >= b:
    if a >= c:
      if a >= d:
        return a
      else:
        return d
    else:
      if c >= d:
        return c
      else:
        return d
  else:
    if b >= c:
      if b >= d:
        return b
      else:
        return d
    else:
      if c >= d:
        return c
      else:
        return d

def compute_inner_alignment_matrix_simple_scoring_int(
  np.int_t[:, :] scoring_matrix,
  int[:] a,
  int[:] b,
  int match_score, int mismatch_score, int gap_score):
  cdef int m = len(a) + 1
  cdef int n = len(b) + 1
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

def compute_inner_alignment_matrix_simple_scoring_any(
  np.int_t[:, :] scoring_matrix,
  a,
  b,
  int match_score, int mismatch_score, int gap_score):
  cdef list ca = list(a)
  cdef list cb = list(b)
  cdef int m = len(ca) + 1
  cdef int n = len(cb) + 1
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

def compute_inner_alignment_matrix_scoring_fn_any(
  np.int_t[:, :] scoring_matrix,
  a,
  b,
  scoring_fn, int gap_score):
  cdef list ca = list(a)
  cdef list cb = list(b)
  cdef int m = len(ca) + 1
  cdef int n = len(cb) + 1
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
