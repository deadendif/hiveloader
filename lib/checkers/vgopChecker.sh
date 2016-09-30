#!/bin/bash

checkersPath=$(cd $(dirname $0); pwd)
source ${checkersPath}"/baseChecker.sh"


# [Overwrite] 生成文件的的校验信息
# @param $1: 数据文件路径
# @echo 文件校验信息: 文件名€文件大小€记录数€数据日期€采集生成时间
function buildCheckInfo() {
    infoList=($(basename $1) $(getFileSize $1) $(getFileLines $1) ${recordDate} $(getFileTime $1))
    info=""
    for((i=0;i<${#infoList[@]};i++))
    do
        if [ $((i+1)) == ${#infoList[@]} ]
        then
            info=${info}${infoList[i]}
        else
            info=${info}${infoList[i]}${separator}
        fi
    done
    echo ${info}
}

main $@
