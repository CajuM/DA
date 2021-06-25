#!/usr/bin/env python

import json
import gzip
import sys

import boto3

from botocore import UNSIGNED
from botocore.client import Config

# Boto3 anonymous login to common crawl
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

for line in sys.stdin:
    line = json.loads(line)
    if 'url' not in line:
        continue

    filename = line['filename']
    offset = int(line['offset'])
    length = int(line['length'])

    offset_end = offset + length - 1
    byte_range = f'bytes={offset}-{offset_end}'
    gzipped_text = s3.get_object(Bucket='commoncrawl', Key=filename, Range=byte_range)['Body'].read()

    sys.stdout.buffer.write(gzip.decompress(gzipped_text))
