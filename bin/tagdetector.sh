#!/bin/bash

base_dir=$(cd $(dirname $(dirname $0)); pwd)
echo "Hive loader location: "${base_dir}
export PYTHONPATH=${base_dir}
cd ${base_dir}

python src/tagDetector/TagDetector.py
