flatten = lambda l: [item for sublist in l for item in sublist]

iter_flatten = lambda l: (item for sublist in l for item in sublist)

def filter_truthy(list_of_something):
  return [l for l in list_of_something if l]

def strip_all(list_of_strings):
  return [(s or '').strip() for s in list_of_strings if s]
