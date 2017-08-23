# Experimental Tools

## Pre-requisites

* Python 3

## Setup

```bash
pip install -r requirements.txt
```

## lxml to svg

Convert _Layout XML_ produced by Grobid's fork of [pdf2xml or pdftoxml](https://github.com/kermitt2/pdf2xml) to SVG.

Because SVG doesn't support multiple pages yet, seperate files will be created.

For example to convert a PDF to LXML you may run:
```bash
pdftoxml test.pdf test.lxml
```

Convert the file to SVG:
```bash
python -m sciencebeam_experiments.lxmlToSvg --lxml-path test.lxml
```

That will create `test-page1.svg`, `test-page2.svg`, etc.
