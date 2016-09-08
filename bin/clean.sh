#!/bin/bash

base_dir=$(cd $(dirname $(dirname $0)); pwd)
echo "[INFO] Hive loader location: "${base_dir}

if [ "${base_dir}" == "/" ];then
    echo "[INFO] Wrong hive loader location!"
    exit 1
fi

cd ${base_dir}

rmdirOnly() {
    if [ $# == 0 ];then
        echo "[WARN] Usage: rmdirOnly <dir1> [<dir2> ...]"
        return
    fi


    for d in $@;do
        if [ -d $d ]
        then
            find $d -maxdepth 1 -mindepth 1 -type d -exec rm -rf '{}' \;
            echo "[INFO] "$d"/ cleaned"
        else
            echo "[INFO] "$d"/ not exist"
        fi
    done
}


rmdirOnly data logs tmp
echo "[INFO] Done"
