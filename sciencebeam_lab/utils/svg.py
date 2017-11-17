from __future__ import print_function

import cairosvg

from six import BytesIO

class SvgToPngConverter(object):
  def __init__(self, dpi=96):
    self.dpi = dpi

  def svg_to_png_bytes(self, source_bytes):
    return cairosvg.svg2png(
      bytestring=source_bytes,
      write_to=None
    )

  def svg_to_png_fp(self, source_fp, target_fp):
    cairosvg.svg2png(
      file_obj=source_fp,
      write_to=target_fp
    )

if __name__ == '__main__':
  svg_filename = '.temp/test.svg'
  png_filename = '.temp/test.png'

  out = BytesIO()
  with open(svg_filename, 'rb') as f:
    SvgToPngConverter().svg_to_png_fp(
      f,
      out
    )
  with open(png_filename, 'wb') as f:
    data = out.getvalue()
    f.write(data)
    print('written %s bytes to %s' % (format(len(data), ','), png_filename))

  with open(svg_filename, 'rb') as f:
    assert SvgToPngConverter().svg_to_png_bytes(
      f.read()
    ) == data
