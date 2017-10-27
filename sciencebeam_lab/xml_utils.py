
def get_text_content(node):
  '''
  Strip tags and return text content
  '''
  return ''.join(node.itertext())

def get_text_content_list(nodes):
  return [get_text_content(node) for node in nodes]
