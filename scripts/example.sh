#!/usr/bin/env bash

export REF_CONFIGURATION=$PWD/.ref

rm $REF_CONFIGURATION/db/cmip_ref.db

ref datasets ingest --source-type cmip6 tests/test-data/sample-data/CMIP6
ref datasets ingest --source-type obs4mips tests/test-data/sample-data/obs4MIPs

ref --verbose solve
