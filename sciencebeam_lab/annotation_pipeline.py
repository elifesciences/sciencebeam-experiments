from __future__ import absolute_import

import argparse
import os
import subprocess
import errno
import logging
from io import BytesIO
from zipfile import ZipFile, ZIP_DEFLATED

from lxml import etree

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from sciencebeam_lab.beam_utils.utils import (
  TransformAndLog
)

from sciencebeam_lab.lxml_to_svg import (
  iter_svg_pages_for_lxml
)

def get_logger():
  return logging.getLogger(__name__)

def create_fn_api_runner():
  from apache_beam.runners.portability.fn_api_runner import FnApiRunner
  return FnApiRunner()

def find_matching_filenames(pattern):
  return [x.path for x in FileSystems.match([pattern])[0].metadata_list]

def read_all_from_path(path):
  buffer_size = 4096 * 1024
  with FileSystems.open(path) as f:
    out = BytesIO()
    while True:
      buf = f.read(buffer_size)
      if not buf:
        break
      out.write(buf)
    return out.getvalue()

def convert_and_annotate_lxml_content(lxml_content):
  lxml_root = etree.fromstring(lxml_content)
  svg_roots = list(iter_svg_pages_for_lxml(lxml_root))
  return [etree.tostring(svg_root) for svg_root in svg_roots]

def relative_path(base_path, path):
  if not base_path.endswith('/'):
    base_path += '/'
  return path[len(base_path):] if path.startswith(base_path) else path

def mkdirs_if_not_exists(path):
  if not FileSystems.exists(path):
    try:
      get_logger().info('attempting to create directory: %s', path)
      FileSystems.mkdirs(path)
    except IOError:
      if not FileSystems.exists(path):
        raise

def basename(path):
  return FileSystems.split(path)[0]

def output_path_for_data_path(base_data_path, data_path, output_path):
  return FileSystems.join(
    output_path,
    relative_path(base_data_path, data_path).replace('.lxml', '.svg.zip')
  )

def save_svg_roots(output_filename, svg_pages):
  mkdirs_if_not_exists(basename(output_filename))
  with FileSystems.create(output_filename) as f:
    with ZipFile(f, 'w', compression=ZIP_DEFLATED) as zf:
      for i, svg_page in enumerate(svg_pages):
        svg_page_filename = 'page-%s.svg' % (1 + i)
        get_logger().debug('svg_page_filename: %s', svg_page_filename)
        data = svg_page
        zf.writestr(svg_page_filename, data)
    return output_filename

def configure_pipeline(p, opt):
  _ = (
    p |
    beam.Create([opt.lxml_path]) |
    "FindFiles" >> TransformAndLog(
      beam.FlatMap(lambda pattern: find_matching_filenames(pattern)),
      log_prefix='files: ',
      log_level='debug'
    ) |
    "ReadFileContent" >> beam.Map(lambda filename: {
      'filename': filename,
      'content': read_all_from_path(filename)
    }) |
    "ConvertAndAnnotate" >> beam.Map(lambda v: {
      'filename': v['filename'],
      'svg_pages': list(convert_and_annotate_lxml_content(v['content']))
    }) |
    "SaveOutput" >> TransformAndLog(
        beam.Map(lambda v: {
        'filename': v['filename'],
        'output_filename': save_svg_roots(
          output_path_for_data_path(
            opt.base_data_path,
            opt.lxml_path,
            opt.output_path
          ),
          v['svg_pages']
        )
      }),
      log_fn=lambda x: get_logger().info('result: %s', x)
    )
  )

def get_cloud_project():
  cmd = [
    'gcloud', '-q', 'config', 'list', 'project',
    '--format=value(core.project)'
  ]
  with open(os.devnull, 'w') as dev_null:
    try:
      res = subprocess.check_output(cmd, stderr=dev_null).strip()
      if not res:
        raise Exception(
          '--cloud specified but no Google Cloud Platform '
          'project found.\n'
          'Please specify your project name with the --project '
          'flag or set a default project: '
          'gcloud config set project YOUR_PROJECT_NAME'
        )
      return res
    except OSError as e:
      if e.errno == errno.ENOENT:
        raise Exception(
          'gcloud is not installed. The Google Cloud SDK is '
          'necessary to communicate with the Cloud ML service. '
          'Please install and set up gcloud.'
        )
      raise

def parse_args(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--data-path', type=str, required=True,
    help='base data path'
  )
  parser.add_argument(
    '--lxml-path', type=str, required=True,
    help='path to lxml file'
  )
  parser.add_argument(
    '--output-path', required=False,
    help='Output directory to write results to.'
  )
  parser.add_argument(
    '--runner',
    required=False,
    default=None,
    help='Runner.'
  )
  parser.add_argument(
    '--cloud',
    default=False,
    action='store_true'
  )
  parser.add_argument(
    '--project',
    type=str,
    help='The cloud project name to be used for running this pipeline'
  )
  parser.add_argument(
    '--num_workers',
    default=10,
    type=int,
    help='The number of workers.'
  )
  # parsed_args, other_args = parser.parse_known_args(argv)
  parsed_args = parser.parse_args(argv)

  parsed_args.base_data_path = parsed_args.data_path.replace('/*/', '/')

  if not parsed_args.output_path:
    parsed_args.output_path = os.path.join(
      os.path.dirname(parsed_args.base_data_path),
      os.path.basename(parsed_args.base_data_path + '-results')
    )
  if parsed_args.num_workers:
    parsed_args.autoscaling_algorithm = 'NONE'
    parsed_args.max_num_workers = parsed_args.num_workers
  parsed_args.setup_file = './setup.py'

  if parsed_args.cloud:
    # Flags which need to be set for cloud runs.
    default_values = {
      'project':
        get_cloud_project(),
      'temp_location':
        os.path.join(os.path.dirname(parsed_args.output_path), 'temp'),
      'runner':
        'DataflowRunner',
      'save_main_session':
        True,
    }
  else:
    # Flags which need to be set for local runs.
    default_values = {
      'runner': 'DirectRunner',
    }

  get_logger().info('default_values: %s', default_values)
  for kk, vv in default_values.iteritems():
    if kk not in parsed_args or not vars(parsed_args)[kk]:
      vars(parsed_args)[kk] = vv
  get_logger().info('parsed_args: %s', parsed_args)

  return parsed_args

def run(argv=None):
  """Main entry point; defines and runs the tfidf pipeline."""
  known_args = parse_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions.from_dictionary(vars(known_args))
  pipeline_options.view_as(SetupOptions).save_main_session = True

  runner = known_args.runner
  if runner == 'FnApiRunner':
    runner = create_fn_api_runner()

  with beam.Pipeline(runner, options=pipeline_options) as p:
    configure_pipeline(p, known_args)

    # Execute the pipeline and wait until it is completed.


if __name__ == '__main__':
  logging.basicConfig(level='INFO')

  run()
