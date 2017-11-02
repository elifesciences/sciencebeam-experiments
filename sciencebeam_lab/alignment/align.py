import logging
from collections import deque
from itertools import islice
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager

import numpy as np

from six import (
  with_metaclass,
  string_types,
  binary_type
)

try:
  from sciencebeam_lab.alignment.align_fast_utils import ( # pylint: disable=E0611
    native_compute_inner_alignment_matrix_simple_scoring_int,
    native_compute_inner_alignment_matrix_simple_scoring_any,
    native_compute_inner_alignment_matrix_scoring_fn_any,
    native_alignment_matrix_single_path_traceback
  )
  native_enabled = True
except ImportError:
  logging.getLogger(__name__).warning('fast implementation not available')
  native_enabled = False

def get_logger():
  return logging.getLogger(__name__)

@contextmanager
def require_native(required=True):
  global native_enabled # pylint: disable=W0603
  was_enabled = native_enabled
  native_enabled = required
  yield
  native_enabled = was_enabled

def _is_array_of_type(a, dtype):
  return np.issubdtype(a.dtype, dtype)

# fallback implementation
def compute_inner_alignment_matrix_simple_scoring_py(
  scoring_matrix, a, b, match_score, mismatch_score, gap_score):
  m = len(a) + 1
  n = len(b) + 1
  for i in range(1, m):
    for j in range(1, n):
      scoring_matrix[i, j] = max(
        0,

        # Match elements.
        scoring_matrix[i - 1, j - 1] +
        (match_score if a[i - 1] == b[j - 1] else mismatch_score),

        # Gap on sequenceA.
        scoring_matrix[i, j - 1] + gap_score,

        # Gap on sequenceB.
        scoring_matrix[i - 1, j] + gap_score
      )

# fallback implementation
def compute_inner_alignment_matrix_scoring_fn_py(
  scoring_matrix, a, b, scoring_fn, gap_score):
  m = len(a) + 1
  n = len(b) + 1
  for i in range(1, m):
    for j in range(1, n):
      scoring_matrix[i, j] = max(
        0,

        # Match elements.
        scoring_matrix[i - 1, j - 1] +
        scoring_fn(a[i - 1], b[j - 1]),

        # Gap on sequenceA.
        scoring_matrix[i, j - 1] + gap_score,

        # Gap on sequenceB.
        scoring_matrix[i - 1, j] + gap_score
      )

def compute_inner_alignment_matrix_simple_scoring(
  scoring_matrix, a, b, match_score, mismatch_score, gap_score):
  try:
    if (
      native_enabled and
      _is_array_of_type(a, np.int32) and _is_array_of_type(b, np.int32)
    ):
      native_compute_inner_alignment_matrix_simple_scoring_int(
        scoring_matrix, a, b,
        match_score, mismatch_score, gap_score
      )
      return
    elif native_enabled:
      native_compute_inner_alignment_matrix_simple_scoring_any(
        scoring_matrix, a, b,
        match_score, mismatch_score, gap_score
      )
      return
  except AttributeError:
    pass
  compute_inner_alignment_matrix_simple_scoring_py(
    scoring_matrix, a, b,
    match_score, mismatch_score, gap_score
  )

def compute_inner_alignment_matrix_custom_scoring(
  scoring_matrix, a, b, scoring_fn, gap_score):

  if native_enabled:
    native_compute_inner_alignment_matrix_scoring_fn_any(
      scoring_matrix, a, b,
      scoring_fn, gap_score
    )
  else:
    compute_inner_alignment_matrix_scoring_fn_py(
      scoring_matrix, a, b,
      scoring_fn, gap_score
    )

def compute_inner_alignment_matrix(
  scoring_matrix, a, b, scoring):
  if isinstance(scoring, CustomScoring):
    compute_inner_alignment_matrix_custom_scoring(
      scoring_matrix, a, b,
      scoring.scoring_fn, scoring.gap_score
    )
  else:
    compute_inner_alignment_matrix_simple_scoring(
      scoring_matrix, a, b,
      scoring.match_score, scoring.mismatch_score, scoring.gap_score
    )

def _next_locs(score_matrix, i, j):
  diag_score = score_matrix[i - 1][j - 1]
  up_score = score_matrix[i - 1][j]
  left_score = score_matrix[i][j - 1]
  max_score = max(diag_score, up_score, left_score)
  if max_score == 0 or diag_score == 0:
    return []
  if diag_score == max_score:
    return [(i - 1, j - 1)]
  locs = []
  if up_score == max_score:
    locs.append((i - 1, j))
  if left_score == max_score:
    locs.append((i, j - 1))
  return locs

def alignment_matrix_traceback_py(score_matrix, start_locs, limit):
  # Using LinkedListNode to cheaply branch off to multiple paths
  pending_roots = deque([
    LinkedListNode(tuple(loc))
    for loc in start_locs
  ])
  while len(pending_roots) > 0:
    n = pending_roots.pop()
    i, j = n.data
    next_locs = _next_locs(score_matrix, i, j)
    if len(next_locs) == 0:
      yield n
    else:
      pending_roots.extend([
        LinkedListNode(next_loc, n)
        for next_loc in next_locs
      ])

def alignment_matrix_traceback(score_matrix, start_locs, limit):
  if native_enabled and limit == 1:
    yield native_alignment_matrix_single_path_traceback(
      score_matrix, start_locs[0]
    )
  else:
    paths = alignment_matrix_traceback_py(
      score_matrix, start_locs, limit
    )
    if limit:
      paths = islice(paths, limit)
    for path in paths:
      yield path

class SimpleScoring(object):
  def __init__(self, match_score, mismatch_score, gap_score):
    self.match_score = match_score
    self.mismatch_score = mismatch_score
    self.gap_score = gap_score

class CustomScoring(object):
  def __init__(self, scoring_fn, gap_score):
    self.scoring_fn = scoring_fn
    self.gap_score = gap_score

class LinkedListNode(object):
  def __init__(self, data, next_node=None):
    self.data = data
    self.next_node = next_node

  def __str__(self):
    if self.next_node is not None:
      return str(self.data) + ' -> ' + str(self.next_node)
    else:
      return str(self.data)

  def __iter__(self):
    yield self.data
    next_node = self.next_node
    while next_node is not None:
      yield next_node.data
      next_node = next_node.next_node

def _path_to_matching_blocks(path, a, b):
  block_ai = 0
  block_bi = 0
  block_size = 0
  for ai, bi in ((ai_ - 1, bi_ - 1) for ai_, bi_ in path):
    if a[ai] == b[bi]:
      if block_size and block_ai + block_size == ai and block_bi + block_size == bi:
        block_size += 1
      else:
        if block_size:
          yield (block_ai, block_bi, block_size)
        block_ai = ai
        block_bi = bi
        block_size = 1
  if block_size:
    yield (block_ai, block_bi, block_size)

def _as_np_array(s):
  if isinstance(s, binary_type):
    return np.array(list(s), dtype=np.int32)
  if isinstance(s, string_types):
    return np.array([ord(c) for c in s], dtype=np.int32)
  return np.asarray(s)

wrap_sequence = _as_np_array

class AbstractSequenceMatcher(object, with_metaclass(ABCMeta)):
  def __init__(self, a, b, scoring):
    self.a = a
    self.b = b
    self.scoring = scoring
    self._alignment_matrix = None
    self._a = _as_np_array(a)
    self._b = _as_np_array(b)

  @abstractmethod
  def _computer_alignment_matrix(self):
    pass

  def _get_alignment_matrix(self):
    if self._alignment_matrix is None:
      self._alignment_matrix = self._computer_alignment_matrix()
    return self._alignment_matrix

  def get_multiple_matching_blocks(self, limit=None):
    score_matrix = self._get_alignment_matrix()
    max_score = score_matrix.max()

    max_score_loc = np.argwhere(score_matrix == max_score)
    paths = alignment_matrix_traceback(score_matrix, max_score_loc, limit=limit or 0)
    return (
      list(_path_to_matching_blocks(path, self.a, self.b))
      for path in paths
    )

  def get_matching_blocks(self):
    for matching_blocks in self.get_multiple_matching_blocks(limit=1):
      return list(matching_blocks) + [(len(self.a), len(self.b), 0)]
    return [(len(self.a), len(self.b), 0)]

class LocalSequenceMatcher(AbstractSequenceMatcher):
  """
  Local sequence matcher using Smith-Waterman algorithm
  """

  def _computer_alignment_matrix(self):
    m = len(self._a) + 1
    n = len(self._b) + 1
    scoring_matrix = np.empty((m, n), dtype=int)
    scoring_matrix[:, 0] = 0
    scoring_matrix[0, :] = 0
    compute_inner_alignment_matrix(
      scoring_matrix,
      self._a, self._b,
      self.scoring
    )
    return scoring_matrix
