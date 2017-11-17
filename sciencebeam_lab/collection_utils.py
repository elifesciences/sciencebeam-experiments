from six import iteritems

flatten = lambda l: [item for sublist in l for item in sublist]

iter_flatten = lambda l: (item for sublist in l for item in sublist)

def filter_truthy(list_of_something):
  return [l for l in list_of_something if l]

def strip_all(list_of_strings):
  return [(s or '').strip() for s in list_of_strings if s]

def remove_key_from_dict(d, key):
  return {k: v for k, v in iteritems(d) if k != key}

def extract_from_dict(d, key, default_value=None):
  return d.get(key, default_value), remove_key_from_dict(d, key)

def extend_dict(d, *other_dicts):
  """
  example:

  extend_dict(d1, d2)

  is equivalent to Python 3 syntax:
  {
    **d1,
    **d2
  }
  """
  d = d.copy()
  for other_dict in other_dicts:
    d.update(other_dict)
  return d
