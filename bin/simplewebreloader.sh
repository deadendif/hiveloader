#!/bin/bash

base_dir=$(cd $(dirname $(dirname $0)); pwd)
echo "[INFO] Hive loader location: "${base_dir}

if [ "${base_dir}" == "/" ];then
    echo "[ERROR] Wrong hive loader location!"
    exit 1
fi

# 添加工程目录到搜索路径(关键)
export PYTHONPATH=${base_dir}
cd ${base_dir}

python src/operators/WebReloader.py

