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

from sciencebeam_lab.xml_utils import (
  xml_from_string_with_recover
)

from sciencebeam_lab.utils.stopwatch import (
  StopWatchRecorder
)

from sciencebeam_lab.beam_utils.utils import (
  TransformAndLog,
  MapOrLog
)

from sciencebeam_lab.beam_utils.csv import (
  WriteDictCsv
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

from sciencebeam_lab.alignment.align import (
  native_enabled as align_native_enabled
)

from sciencebeam_lab.matching_annotator import (
  MatchingAnnotator,
  parse_xml_mapping,
  xml_root_to_target_annotations
)

from sciencebeam_lab.visualize_svg_annotation import (
  visualize_svg_annotations
)

from sciencebeam_lab.annotation_evaluation import (
  evaluate_document_by_page,
  DEFAULT_EVALUATION_COLUMNS,
  to_csv_dict_rows as to_annotation_evaluation_csv_dict_rows
)

from sciencebeam_lab.pdf2xml.pdf2xml_wrapper import (
  PdfToXmlWrapper
)

def get_logger():
  return logging.getLogger(__name__)

def dirname(path):
  return FileSystems.split(path)[0]

def basename(path):
  return FileSystems.split(path)[1]

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

def read_pdf_and_convert_to_lxml(path):
  stop_watch_recorder = StopWatchRecorder()

  stop_watch_recorder.start('read contents')
  pdf_content = read_all_from_path(path)

  stop_watch_recorder.start('convert to lxml')
  lxml_content = PdfToXmlWrapper().process_input(
    pdf_content,
    '-blocks -noImageInline -noImage -fullFontName'.split()
  )
  stop_watch_recorder.stop()

  get_logger().info(
    'converted to lxml: path=%s, pdf size=%s, lxml size=%s, timings=[%s]',
    path, format(len(pdf_content), ','), format(len(lxml_content), ','),
    stop_watch_recorder
  )

  return lxml_content

def convert_and_annotate_lxml_content(lxml_content, xml_content, xml_mapping, name=None):
  stop_watch_recorder = StopWatchRecorder()

  stop_watch_recorder.start('parse lxml')
  lxml_root = etree.fromstring(lxml_content)

  # use a more lenient way to parse xml as xml errors are not uncomment
  stop_watch_recorder.start('parse xml')
  xml_root = xml_from_string_with_recover(xml_content)

  stop_watch_recorder.start('extract target annotations')
  target_annotations = xml_root_to_target_annotations(
    xml_root,
    xml_mapping
  )
  stop_watch_recorder.stop()

  annotators = DEFAULT_ANNOTATORS + [MatchingAnnotator(
    target_annotations
  )]
  annotator = Annotator(annotators)

  stop_watch_recorder.start('convert to svg')
  svg_roots = list(iter_svg_pages_for_lxml(lxml_root))

  stop_watch_recorder.start('annotate svg')
  annotator.annotate(SvgStructuredDocument(svg_roots))

  stop_watch_recorder.start('add visualisation')
  svg_roots = [
    visualize_svg_annotations(svg_root)
    for svg_root in svg_roots
  ]
  stop_watch_recorder.stop()

  get_logger().info(
    'processed: name=%s, lxml size=%s, xml size=%s, timings=[%s] (native align impl=%s)',
    name, format(len(lxml_content), ','), format(len(xml_content), ','),
    stop_watch_recorder, align_native_enabled
  )

  return svg_roots

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

def change_ext(path, old_ext, new_ext):
  if old_ext is None:
    old_ext = os.path.splitext(path)[1]
  if old_ext and path.endswith(old_ext):
    return path[:-len(old_ext)] + new_ext
  else:
    return path + new_ext

def save_svg_roots(output_filename, svg_pages):
  mkdirs_if_not_exists(dirname(output_filename))
  with FileSystems.create(output_filename) as f:
    with ZipFile(f, 'w', compression=ZIP_DEFLATED) as zf:
      for i, svg_page in enumerate(svg_pages):
        svg_page_filename = 'page-%s.svg' % (1 + i)
        get_logger().debug('svg_page_filename: %s', svg_page_filename)
        data = etree.tostring(svg_page)
        zf.writestr(svg_page_filename, data)
    return output_filename

def save_file_content(output_filename, data):
  mkdirs_if_not_exists(dirname(output_filename))
  # Note: FileSystems.create transparently handles compression based on the file extension
  with FileSystems.create(output_filename) as f:
    f.write(data)
  return output_filename

def configure_pipeline(p, opt):
  xml_mapping = parse_xml_mapping(opt.xml_mapping_path)
  if opt.lxml_path:
    lxml_xml_file_pairs = (
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
        'source_filename': filenames[0],
        'xml_filename': filenames[1],
        'lxml_content': read_all_from_path(filenames[0]),
        'xml_content': read_all_from_path(filenames[1])
      })
    )
  elif opt.pdf_path:
    lxml_xml_file_pairs = (
      p |
      beam.Create([[
        join_if_relative_path(opt.base_data_path, s)
        for s in [opt.pdf_path, opt.xml_path]
      ]]) |
      "FindFilePairs" >> TransformAndLog(
        beam.FlatMap(
          lambda patterns: find_file_pairs_grouped_by_parent_directory(patterns)
        ),
        log_prefix='file pairs: ',
        log_level='debug'
      ) |
      "ReadFileContentAndConvertPdfToLxml" >> MapOrLog(lambda filenames: {
        'source_filename': filenames[0],
        'xml_filename': filenames[1],
        'lxml_content': read_pdf_and_convert_to_lxml(filenames[0]),
        'xml_content': read_all_from_path(filenames[1])
      }, log_fn=lambda e, filenames: (
        get_logger().warning(
          'caucht exception (ignoring item): %s, pdf: %s, xml: %s',
          e, filenames[0], filenames[1]
        )
      ))
    )
  else:
    raise RuntimeError('either lxml-path or pdf-path required')

  if opt.save_lxml:
    _ = (
      lxml_xml_file_pairs |
      "SaveLxml" >> TransformAndLog(
        beam.Map(lambda v: {
          'source_filename': v['source_filename'],
          'xml_filename': v['xml_filename'],
          'output_filename': save_file_content(
            FileSystems.join(
              opt.output_path,
              change_ext(
                relative_path(opt.base_data_path, v['source_filename']),
                None, '.lxml.gz'
              )
            ),
            v['lxml_content']
          )
        }),
        log_fn=lambda x: get_logger().info('saved lxml: %s', x['output_filename'])
      )
    )

  annotation_results = (
    lxml_xml_file_pairs |
    "ConvertAndAnnotate" >> MapOrLog(lambda v: {
      'source_filename': v['source_filename'],
      'xml_filename': v['xml_filename'],
      'svg_pages': list(convert_and_annotate_lxml_content(
        v['lxml_content'], v['xml_content'], xml_mapping,
        name=v['source_filename']
      ))
    }, log_fn=lambda e, v: (
      get_logger().warning(
        'caucht exception (ignoring item): %s, source: %s, xml: %s',
        e, v['source_filename'], v['xml_filename']
      )
    ))
  )
  _ = (
    annotation_results |
    "SaveOutput" >> TransformAndLog(
      beam.Map(lambda v: {
        'source_filename': v['source_filename'],
        'xml_filename': v['xml_filename'],
        'output_filename': save_svg_roots(
          FileSystems.join(
            opt.output_path,
            change_ext(
              relative_path(opt.base_data_path, v['source_filename']),
              None, '.svg.zip'
            )
          ),
          v['svg_pages']
        )
      }),
      log_fn=lambda x: get_logger().info('saved result: %s', x['output_filename'])
    )
  )
  if opt.annotation_evaluation_csv:
    annotation_evaluation_csv_name, annotation_evaluation_ext = (
      os.path.splitext(opt.annotation_evaluation_csv)
    )
    _ = (
      annotation_results |
      "EvaluateAnnotations" >> TransformAndLog(
        beam.Map(lambda v: {
          'source_filename': v['source_filename'],
          'xml_filename': v['xml_filename'],
          'annotation_evaluation': evaluate_document_by_page(
            SvgStructuredDocument(v['svg_pages'])
          )
        }),
        log_fn=lambda x: get_logger().info('annotation evaluation result: %s', x)
      ) |
      "FlattenAnotationEvaluationResults" >> beam.FlatMap(
        lambda v: to_annotation_evaluation_csv_dict_rows(
          v['annotation_evaluation'],
          document=basename(v['source_filename'])
        )
      ) |
      "WriteAnnotationEvaluationToCsv" >> WriteDictCsv(
        join_if_relative_path(opt.output_path, annotation_evaluation_csv_name),
        file_name_suffix=annotation_evaluation_ext,
        columns=DEFAULT_EVALUATION_COLUMNS
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

def add_main_args(parser):
  parser.add_argument(
    '--data-path', type=str, required=True,
    help='base data path'
  )

  source_group = parser.add_mutually_exclusive_group(required=True)
  source_group.add_argument(
    '--lxml-path', type=str, required=False,
    help='path to lxml file(s)'
  )
  source_group.add_argument(
    '--pdf-path', type=str, required=False,
    help='path to pdf file(s) (alternative to lxml)'
  )

  parser.add_argument(
    '--save-lxml', default=False, action='store_true',
    help='save generated lxml (if using pdf as an input)'
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
    '--annotation-evaluation-csv', type=str, required=False,
    help='Annotation evaluation CSV output file'
  )
  parser.add_argument(
    '--output-path', required=False,
    help='Output directory to write results to.'
  )

def process_main_args(parser, parsed_args):
  parsed_args.base_data_path = parsed_args.data_path.replace('/*/', '/')

  if not parsed_args.output_path:
    parsed_args.output_path = os.path.join(
      os.path.dirname(parsed_args.base_data_path),
      os.path.basename(parsed_args.base_data_path + '-results')
    )

  if parsed_args.save_lxml and not parsed_args.pdf_path:
    parser.error('--save-lxml only valid with --pdf-path')

def add_cloud_args(parser):
  parser.add_argument(
    '--cloud',
    default=False,
    action='store_true'
  )
  parser.add_argument(
    '--runner',
    required=False,
    default=None,
    help='Runner.'
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

def process_cloud_args(parsed_args, output_path):
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
        os.path.join(os.path.dirname(output_path), 'temp'),
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

def parse_args(argv=None):
  parser = argparse.ArgumentParser()
  add_main_args(parser)
  add_cloud_args(parser)

  # parsed_args, other_args = parser.parse_known_args(argv)
  parsed_args = parser.parse_args(argv)

  process_main_args(parser, parsed_args)
  process_cloud_args(parsed_args, parsed_args.output_path)

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
