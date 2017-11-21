# Experimental Tools

## Pre-requisites

* Python 3

## Setup

```bash
pip install -r requirements.txt
```

# Cython

Run:

```bash
python setup.py build_ext --inplace
```

For more information see [Cython Build Documentation](http://docs.cython.org/en/latest/src/quickstart/build.html).

## lxml to svg

Convert _Layout XML_ produced by Grobid's fork of [pdf2xml or pdftoxml](https://github.com/kermitt2/pdf2xml) to SVG.

Because SVG doesn't support multiple pages yet, separate files will be created.

For example to convert a PDF to LXML you may run:
```bash
pdftoxml test.pdf test.lxml
```

Convert the file to SVG:
```bash
python -m sciencebeam_lab.preprocess.lxml_to_svg --lxml-path test.lxml
```

That will create `test-page1.svg`, `test-page2.svg`, etc.
