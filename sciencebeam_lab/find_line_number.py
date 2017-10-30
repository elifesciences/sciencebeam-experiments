import logging
from collections import Counter

def get_logger():
  return logging.getLogger(__name__)

def _find_line_number_token_candidates(structured_document, page):
  for line in structured_document.get_lines_of_page(page):
    text_tokens = sorted(
      structured_document.get_tokens_of_line(line),
      key=lambda t: structured_document.get_x(t)
    )
    if text_tokens:
      token = text_tokens[0]
      token_text = structured_document.get_text(token)
      if token_text and token_text.isdigit():
        yield token

def find_line_number_tokens(structured_document):
  for page in structured_document.get_pages():
    line_number_candidates = list(_find_line_number_token_candidates(
      structured_document,
      page
    ))
    # we need more than two lines
    if len(line_number_candidates) > 2:
      c = Counter((
        round(float(structured_document.get_x(t)))
        for t in line_number_candidates
      ))
      get_logger().debug('counter: %s', c)
      most_common_x, most_common_count = c.most_common(1)[0]
      get_logger().debug('most_common: x: %s (count: %s)', most_common_x, most_common_count)
      for token in line_number_candidates:
        x = float(token.attrib['x'])
        if abs(x - most_common_x) < 10:
          get_logger().debug(
            'line no token: most_common_x=%s, token_x=%s, token=%s',
            most_common_x, structured_document.get_x(token), structured_document.get_text(token)
          )
          yield token
        else:
          get_logger().debug(
            'line no exeeds threshold token: most_common_x=%s, token_x=%s, token=%s',
            most_common_x, structured_document.get_x(token), structured_document.get_text(token)
          )
