from __future__ import absolute_import

import argparse
import os
import logging
from io import BytesIO
from zipfile import ZipFile, ZIP_DEFLATED
from itertools import groupby
from functools import reduce

from lxml import etree

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from sciencebeam_lab.xml_utils import (
  xml_from_string_with_recover
)

from sciencebeam_lab.collection_utils import (
  extend_dict,
  remove_keys_from_dict
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

from sciencebeam_lab.beam_utils.io import (
  read_all_from_path,
  dirname,
  basename,
  find_matching_filenames,
  mkdirs_if_not_exists,
  save_file_content
)

from sciencebeam_lab.beam_utils.main import (
  add_cloud_args,
  process_cloud_args
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

from sciencebeam_lab.color_map import (
  parse_color_map_from_file
)

from sciencebeam_lab.blockify_annotations import (
  annotation_document_page_to_annotation_blocks,
  merge_blocks,
  expand_blocks,
  annotated_blocks_to_image
)

from sciencebeam_lab.annotation_evaluation import (
  evaluate_document_by_page,
  DEFAULT_EVALUATION_COLUMNS,
  to_csv_dict_rows as to_annotation_evaluation_csv_dict_rows
)

from sciencebeam_lab.pdf import (
  PdfToLxmlWrapper,
  PdfToPng
)

def get_logger():
  return logging.getLogger(__name__)

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
    list(find_matching_filenames(pattern)) for pattern in patterns
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

def convert_pdf_bytes_to_lxml(pdf_content, path=None):
  stop_watch_recorder = StopWatchRecorder()

  stop_watch_recorder.start('convert to lxml')
  lxml_content = PdfToLxmlWrapper().process_input(
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

def change_ext(path, old_ext, new_ext):
  if old_ext is None:
    old_ext = os.path.splitext(path)[1]
  if old_ext and path.endswith(old_ext):
    return path[:-len(old_ext)] + new_ext
  else:
    return path + new_ext

def save_pages(output_filename, ext, bytes_by_page):
  mkdirs_if_not_exists(dirname(output_filename))
  with FileSystems.create(output_filename) as f:
    with ZipFile(f, 'w', compression=ZIP_DEFLATED) as zf:
      for i, data in enumerate(bytes_by_page):
        page_filename = 'page-%s%s' % (1 + i, ext)
        get_logger().debug('page_filename: %s', page_filename)
        zf.writestr(page_filename, data)
    return output_filename

def save_svg_roots(output_filename, svg_pages):
  return save_pages(output_filename, '.svg', (
    etree.tostring(svg_page)
    for svg_page in svg_pages
  ))

def pdf_bytes_to_png_pages(pdf_bytes, dpi, image_size):
  pdf_to_png = PdfToPng(dpi=dpi, image_size=image_size)
  return (
    fp.read()
    for fp in pdf_to_png.iter_pdf_bytes_to_png_fp(pdf_bytes)
  )

def svg_page_to_blockified_png_bytes(svg_page, color_map, image_size=None):
  structured_document = SvgStructuredDocument(svg_page)
  blocks = expand_blocks(
    merge_blocks(
      annotation_document_page_to_annotation_blocks(
        structured_document,
        structured_document.get_pages()[0]
      )
    )
  )
  viewbox = svg_page.attrib.get('viewBox')
  if not viewbox:
    raise RuntimeError(
      'viewbox missing on svg, available attributes: %s' % svg_page.attrib.keys()
    )
  _, _, width, height = viewbox.split()
  image = annotated_blocks_to_image(
    blocks, color_map,
    width=int(width), height=int(height), background='white',
    scale_to_size=image_size
  )
  out = BytesIO()
  image.save(out, 'png')
  return out.getvalue()

def configure_pipeline(p, opt):
  image_size = (
    (opt.image_width, opt.image_height)
    if opt.image_width and opt.image_height
    else None
  )
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
    pdf_xml_file_pairs = (
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
      "ReadFileContent" >> beam.Map(lambda filenames: {
        'source_filename': filenames[0],
        'xml_filename': filenames[1],
        'pdf_content': read_all_from_path(filenames[0]),
        'xml_content': read_all_from_path(filenames[1])
      })
    )

    lxml_xml_file_pairs = (
      pdf_xml_file_pairs |
      "ConvertPdfToLxml" >> MapOrLog(lambda v: remove_keys_from_dict(
        extend_dict(v, {
          'lxml_content': convert_pdf_bytes_to_lxml(
            v['pdf_content'], path=v['source_filename']
          )
        }),
        # we don't need the pdf_content unless we are writing tf_records
        None if opt.save_tfrecords else {'pdf_content'}
      ), log_fn=lambda e, v: (
        get_logger().warning(
          'caught exception (ignoring item): %s, pdf: %s, xml: %s',
          e, v['source_filename'], v['xml_filename'], exc_info=e
        )
      ))
    )
  else:
    raise RuntimeError('either lxml-path or pdf-path required')

  if opt.save_png or opt.save_tfrecords:
    with_pdf_png_pages = (
      (lxml_xml_file_pairs if opt.save_tfrecords else pdf_xml_file_pairs) |
      "ConvertPdfToPng" >> MapOrLog(lambda v: remove_keys_from_dict(
        extend_dict(v, {
          'pdf_png_pages':  list(pdf_bytes_to_png_pages(
            v['pdf_content'],
            dpi=opt.png_dpi,
            image_size=image_size
          ))
        }),
        {'pdf_content'} # we no longer need the pdf_content
      ))
    )

    if opt.save_png:
      _ = (
        with_pdf_png_pages |
        "SavePdfToPng" >> TransformAndLog(
          beam.Map(lambda v: save_pages(
            FileSystems.join(
              opt.output_path,
              change_ext(
                relative_path(opt.base_data_path, v['source_filename']),
                None, '.png.zip'
              )
            ),
            '.png',
            v['pdf_png_pages']
          )),
          log_fn=lambda x: get_logger().info('saved result: %s', x)
        )
      )

  if opt.save_lxml:
    _ = (
      lxml_xml_file_pairs |
      "SaveLxml" >> TransformAndLog(
        beam.Map(lambda v: save_file_content(
          FileSystems.join(
            opt.output_path,
            change_ext(
              relative_path(opt.base_data_path, v['source_filename']),
              None, '.lxml.gz'
            )
          ),
          v['lxml_content']
        )),
        log_fn=lambda x: get_logger().info('saved lxml: %s', x)
      )
    )

  annotation_results = (
    (with_pdf_png_pages if opt.save_tfrecords else lxml_xml_file_pairs) |
    "ConvertAndAnnotate" >> MapOrLog(lambda v: remove_keys_from_dict(
      extend_dict(v, {
        'svg_pages': list(convert_and_annotate_lxml_content(
          v['lxml_content'], v['xml_content'], xml_mapping,
          name=v['source_filename']
        ))
      }),
      # Won't need the XML anymore
      {'lxml_content', 'xml_content'}
    ), log_fn=lambda e, v: (
      get_logger().warning(
        'caught exception (ignoring item): %s, source: %s, xml: %s',
        e, v['source_filename'], v['xml_filename'], exc_info=e
      )
    ))
  )

  _ = (
    annotation_results |
    "SaveOutput" >> TransformAndLog(
      beam.Map(lambda v: save_svg_roots(
        FileSystems.join(
          opt.output_path,
          change_ext(
            relative_path(opt.base_data_path, v['source_filename']),
            None, '.svg.zip'
          )
        ),
        v['svg_pages']
      )),
      log_fn=lambda x: get_logger().info('saved result: %s', x)
    )
  )

  if opt.save_block_png or opt.save_tfrecords:
    color_map = parse_color_map_from_file(opt.color_map)
    with_block_png_pages = (
      annotation_results |
      "GenerateBlockPng" >> beam.Map(lambda v: remove_keys_from_dict(
        extend_dict(v, {
          'block_png_pages': [
            svg_page_to_blockified_png_bytes(svg_page, color_map, image_size=image_size)
            for svg_page in v['svg_pages']
          ]
        }),
        {'svg_pages'}
      ))
    )

    if opt.save_block_png:
      _ = (
        with_block_png_pages |
        "SaveBlockPng" >> TransformAndLog(
          beam.Map(lambda v: save_pages(
            FileSystems.join(
              opt.output_path,
              change_ext(
                relative_path(opt.base_data_path, v['source_filename']),
                None, '.block-png.zip'
              )
            ),
            '.png',
            v['block_png_pages']
          )),
          log_fn=lambda x: get_logger().info('saved result: %s', x)
        )
      )

    if opt.save_tfrecords:
      import tensorflow as tf

      def _bytes_feature(value):
        return tf.train.Feature(bytes_list=tf.train.BytesList(value=value))

      def convert_to_example(input_uri, input_image, annotation_uri, annotation_image):
        input_uri_bytes = _bytes_feature(input_uri.encode('utf-8'))
        input_image_bytes = _bytes_feature(input_image)
        annotation_uri_bytes = _bytes_feature(annotation_uri.encode('utf-8'))
        annotation_image_bytes = _bytes_feature(annotation_image)
        return tf.train.Example(features=tf.train.Features(feature={
          'input_uri': input_uri_bytes,
          'input_image': input_image_bytes,
          'annotation_uri': annotation_uri_bytes,
          'annotation_image': annotation_image_bytes
        }))

      _ = (
        with_block_png_pages |
        'ConvertToTfExamples' >> beam.FlatMap(lambda v: [
          convert_to_example(
            input_uri=v['source_filename'],
            input_image=pdf_png_page,
            annotation_uri=v['source_filename'] + '.annot',
            annotation_image=block_png_page
          )
          for pdf_png_page, block_png_page in zip(v['pdf_png_pages'], v['block_png_pages'])
        ]) |
        'SerializeToString' >> beam.Map(lambda x: x.SerializeToString()) |
        'SaveToTfRecords' >> beam.io.WriteToTFRecord(
          FileSystems.join(opt.output_path, 'data'),
          file_name_suffix='.tfrecord.gz'
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
    '--save-png', default=False, action='store_true',
    help='save png pages of the original pdf'
  )
  parser.add_argument(
    '--png-dpi', type=int, default=90,
    help='dpi of rendered pdf pages'
  )

  parser.add_argument(
    '--image-width', type=int, required=False,
    help='image width of resulting PNGs'
  )
  parser.add_argument(
    '--image-height', type=int, required=False,
    help='image height of resulting PNGs'
  )

  parser.add_argument(
    '--save-block-png', default=False, action='store_true',
    help='save blockified version of the svg as a png'
  )
  parser.add_argument(
    '--color-map', default='color_map.conf',
    help='color map to use (see save-block-png)'
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
    '--save-tfrecords', default=False, action='store_true',
    help='Save TFRecords with PDF PNG and Annotation PNG'
    ' (--image-width and --image-height recommended)'
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

  if parsed_args.save_png and not parsed_args.pdf_path:
    parser.error('--save-png only valid with --pdf-path')

  if parsed_args.save_tfrecords and not parsed_args.pdf_path:
    parser.error('--save-tfrecords only valid with --pdf-path')

  if sum(1 if x else 0 for x in (parsed_args.image_width, parsed_args.image_height)) == 1:
    parser.error('--image-width and --image-height need to be specified together')

def parse_args(argv=None):
  parser = argparse.ArgumentParser()
  add_main_args(parser)
  add_cloud_args(parser)

  # parsed_args, other_args = parser.parse_known_args(argv)
  parsed_args = parser.parse_args(argv)

  process_main_args(parser, parsed_args)
  process_cloud_args(
    parsed_args, parsed_args.output_path,
    name='sciencbeam-lab'
  )

  get_logger().info('parsed_args: %s', parsed_args)

  return parsed_args

def run(argv=None):
  """Main entry point; defines and runs the tfidf pipeline."""
  known_args = parse_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions.from_dictionary(vars(known_args))
  pipeline_options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(known_args.runner, options=pipeline_options) as p:
    configure_pipeline(p, known_args)

    # Execute the pipeline and wait until it is completed.


if __name__ == '__main__':
  logging.basicConfig(level='INFO')

  run()
