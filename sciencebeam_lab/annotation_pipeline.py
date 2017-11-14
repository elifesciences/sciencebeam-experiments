from __future__ import absolute_import

import argparse
import os
import subprocess
import errno
import logging
from io import BytesIO
from zipfile import ZipFile, ZIP_DEFLATED
from itertools import groupby
from functools import reduce

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

from sciencebeam_lab.svg_structured_document import (
  SvgStructuredDocument
)

from sciencebeam_lab.annotator import (
  Annotator,
  DEFAULT_ANNOTATORS
)

from sciencebeam_lab.matching_annotator import (
  MatchingAnnotator,
  parse_xml_mapping,
  xml_root_to_target_annotations
)

def get_logger():
  return logging.getLogger(__name__)

def dirname(path):
  return FileSystems.split(path)[0]

def create_fn_api_runner():
  from apache_beam.runners.portability.fn_api_runner import FnApiRunner
  return FnApiRunner()

def find_matching_filenames(pattern):
  return [x.path for x in FileSystems.match([pattern])[0].metadata_list]

def group_files_by_parent_directory(filenames):
  return {
    k: list(v)
    for k, v in groupby(sorted(filenames), lambda x: os.path.dirname(x))
  }

def zip_by_keys(*dict_list):
  keys = reduce(lambda agg, v: agg | set(v.keys()), dict_list, set())
  return (
    [d.get(k) for d in dict_list]
    for k in sorted(keys)
  )

def find_file_pairs_grouped_by_parent_directory(patterns):
  matching_files_by_pattern = [
    find_matching_filenames(pattern) for pattern in patterns
  ]
  get_logger().info(
    'found number of files %s',
    ', '.join(
      '%s: %d' % (pattern, len(files))
      for pattern, files in zip(patterns, matching_files_by_pattern)
    )
  )
  patterns_without_files = [
    pattern
    for pattern, files in zip(patterns, matching_files_by_pattern)
    if len(files) == 0
  ]
  if patterns_without_files:
    raise RuntimeError('no files found for: %s' % patterns_without_files)
  grouped_files_by_pattern = [
    group_files_by_parent_directory(files) for files in matching_files_by_pattern
  ]
  for files_in_group_by_pattern in zip_by_keys(*grouped_files_by_pattern):
    if all(len(files or []) == 1 for files in files_in_group_by_pattern):
      yield tuple([files[0] for files in files_in_group_by_pattern])
    else:
      get_logger().info(
        'no exclusively matching files found: %s',
        [files for files in files_in_group_by_pattern]
      )

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

def convert_and_annotate_lxml_content(lxml_content, xml_content, xml_mapping):
  lxml_root = etree.fromstring(lxml_content)
  target_annotations = xml_root_to_target_annotations(
    etree.fromstring(xml_content),
    xml_mapping
  )
  annotators = DEFAULT_ANNOTATORS + [MatchingAnnotator(
    target_annotations
  )]
  annotator = Annotator(annotators)
  svg_roots = list(iter_svg_pages_for_lxml(lxml_root))
  annotator.annotate(SvgStructuredDocument(svg_roots))
  return [etree.tostring(svg_root) for svg_root in svg_roots]

def relative_path(base_path, path):
  if not base_path.endswith('/'):
    base_path += '/'
  return path[len(base_path):] if path.startswith(base_path) else path

def is_relative_path(path):
  return not path.startswith('/') and '://' not in path

def join_if_relative_path(base_path, path):
  return FileSystems.join(base_path, path) if is_relative_path(path) else path

def mkdirs_if_not_exists(path):
  if not FileSystems.exists(path):
    try:
      get_logger().info('attempting to create directory: %s', path)
      FileSystems.mkdirs(path)
    except IOError:
      if not FileSystems.exists(path):
        raise

def output_path_for_data_path(base_data_path, data_path, output_path):
  return FileSystems.join(
    output_path,
    relative_path(base_data_path, data_path).replace('.lxml', '.svg.zip')
  )

def save_svg_roots(output_filename, svg_pages):
  mkdirs_if_not_exists(dirname(output_filename))
  with FileSystems.create(output_filename) as f:
    with ZipFile(f, 'w', compression=ZIP_DEFLATED) as zf:
      for i, svg_page in enumerate(svg_pages):
        svg_page_filename = 'page-%s.svg' % (1 + i)
        get_logger().debug('svg_page_filename: %s', svg_page_filename)
        data = svg_page
        zf.writestr(svg_page_filename, data)
    return output_filename

def configure_pipeline(p, opt):
  xml_mapping = parse_xml_mapping(opt.xml_mapping_path)
  _ = (
    p |
    beam.Create([[
      join_if_relative_path(opt.base_data_path, s)
      for s in [opt.lxml_path, opt.xml_path]
    ]]) |
    "FindFilePairs" >> TransformAndLog(
      beam.FlatMap(
        lambda patterns: find_file_pairs_grouped_by_parent_directory(patterns)
      ),
      log_prefix='file pairs: ',
      log_level='debug'
    ) |
    "ReadFileContent" >> beam.Map(lambda filenames: {
      'lxml_filename': filenames[0],
      'xml_filename': filenames[1],
      'lxml_content': read_all_from_path(filenames[0]),
      'xml_content': read_all_from_path(filenames[1])
    }) |
    "ConvertAndAnnotate" >> beam.Map(lambda v: {
      'lxml_filename': v['lxml_filename'],
      'xml_filename': v['xml_filename'],
      'svg_pages': list(convert_and_annotate_lxml_content(
        v['lxml_content'], v['xml_content'], xml_mapping
      ))
    }) |
    "SaveOutput" >> TransformAndLog(
        beam.Map(lambda v: {
        'lxml_filename': v['lxml_filename'],
        'xml_filename': v['xml_filename'],
        'output_filename': save_svg_roots(
          output_path_for_data_path(
            opt.base_data_path,
            opt.lxml_path,
            opt.output_path
          ),
          v['svg_pages']
        )
      }),
      log_fn=lambda x: get_logger().info('saved result: %s', x)
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

def get_default_job_name():
  from getpass import getuser
  from time import gmtime, strftime
  timestamp_str = strftime("%Y%m%d-%H%M%S", gmtime())
  return 'sciencebeam-lab-%s-%s' % (getuser(), timestamp_str)

def parse_args(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--data-path', type=str, required=True,
    help='base data path'
  )
  parser.add_argument(
    '--lxml-path', type=str, required=True,
    help='path to lxml file(s)'
  )
  parser.add_argument(
    '--xml-path', type=str, required=True,
    help='path to xml file(s)'
  )
  parser.add_argument(
    '--xml-mapping-path', type=str, default='annot-xml-front.conf',
    help='path to xml mapping file'
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
  parser.add_argument(
    '--job_name', type=str, required=False,
    help='The name of the cloud job'
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
    if not parsed_args.job_name:
      parsed_args.job_name = get_default_job_name()
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
