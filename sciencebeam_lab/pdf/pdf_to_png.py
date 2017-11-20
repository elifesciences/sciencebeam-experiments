import logging
import os
from subprocess import Popen, PIPE

from backports import tempfile

def get_logger():
  return logging.getLogger(__name__)

class PdfToPng(object):
  def __init__(self, dpi=None):
    self.dpi = dpi

  def iter_pdf_bytes_to_png_fp(self, pdf_bytes):
    cmd = ['pdftoppm', '-png']
    if self.dpi:
      cmd += ['-r', str(self.dpi)]
    cmd += ['-']
    with tempfile.TemporaryDirectory() as path:
      cmd += [os.path.join(path, 'page')]

      p = Popen(cmd, stdout=PIPE, stdin=PIPE, stderr=PIPE)
      try:
        p.stdin.write(pdf_bytes)
      except IOError:
        # we'll check the returncode
        pass

      out, err = p.communicate()
      if p.returncode != 0:
        get_logger().debug(
          'process failed with return code %d: cmd=%s, out=%s, err=%s',
          p.returncode, cmd, out, err
        )
        raise IOError(
          'process failed with return code %d, cmd=%s, err=%s' %
          (p.returncode, cmd, err)
        )

      for filename in sorted(os.listdir(path)):
        with open(os.path.join(path, filename), 'rb') as f:
          yield f

if __name__ == '__main__':
  from sciencebeam_lab.pdf.pdf_to_lxml_wrapper import download_if_not_exist

  logging.basicConfig(level='INFO')

  sample_pdf_url = 'https://rawgit.com/elifesciences/XML-mapping/master/elife-00666.pdf'
  sample_pdf_filename = '.temp/elife-00666.pdf'
  download_if_not_exist(sample_pdf_url, sample_pdf_filename)
  with open(sample_pdf_filename, 'rb') as sample_f:
    sample_pdf_contents = sample_f.read()
  get_logger().info('pdf size: %s bytes', format(len(sample_pdf_contents), ','))
  png_bytes = [f.read() for f in PdfToPng(dpi=30).iter_pdf_bytes_to_png_fp(sample_pdf_contents)]
  get_logger().info('read: total %d (%d files)', sum(len(x) for x in png_bytes), len(png_bytes))
