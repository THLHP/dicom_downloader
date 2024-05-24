#!/bin/bash

# using curl (10 second timeout, retry up to 5 times):
curl -m 10 --retry 5 https://hc-ping.com/hLLDZXOBn0N0ABea5bVJKQ/smk-compressor-chile-dicoms/start

source /root/miniconda3/bin/activate base
cd /root/dicom_downloader/automate/
snakemake --cores 2 --rerun-incomplete -s 05_db_compress_dicoms.smk

curl -m 10 --retry 5 https://hc-ping.com/hLLDZXOBn0N0ABea5bVJKQ/smk-compressor-chile-dicoms/$?