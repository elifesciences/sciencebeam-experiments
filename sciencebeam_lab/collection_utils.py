flatten = lambda l: [item for sublist in l for item in sublist]

iter_flatten = lambda l: (item for sublist in l for item in sublist)

def filter_non_empty(list_of_list):
  return [l for l in list_of_list if l]
