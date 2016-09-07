#!/bin/bash

base_dir=$(cd $(dirname $(dirname $0)); pwd)
echo "[INFO] Hive loader location: "${base_dir}

if [ "${base_dir}" == "/" ];then
    echo "[INFO] Wrong hive loader location!"
    exit 1
fi

cd ${base_dir}

data_dir=${base_dir}"/data/"
if [ -d ${data_dir} ];then
    rm -r ${data_dir}* && echo "[INFO] "${data_dir}" cleaned"
fi


log_dir=${base_dir}"/logs/"
if [ -d ${log_dir} ];then
    rm -r ${log_dir}* && echo "[INFO] "${log_dir}" cleaned" && mkdir logs/tagsLoader logs/tagsDetector logs/webReloader
fi

tmp_dir=${base_dir}"/tmp/"
if [ -d ${tmp_dir} ];then
    rm -r ${tmp_dir}* && echo "[INFO] "${tmp_dir}" cleaned"
fi


echo "[INFO] Done"
