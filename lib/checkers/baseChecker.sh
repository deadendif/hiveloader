#!/bin/bash


# 解析参数
# @param $1: 数据文件所在目录
# @param $2: 数据文件通配符
# @param $3: 校验文件名
# @param $4: 校验文件字段分隔符
# @param $5: 账单时间
function parseParams() {
    if [ $# != 5 ]
    then
        echo "[ERROR] Params wrong: params=$@"
        return 1
    else
        dirPath=$1
        fileNamePattern=$2
        checkFileName=$3
        separator=$4
        recordDate=$5
        return 0
    fi
}


# 生成文件的的校验信息
function check() {
    if [ ! -d ${dirPath} ];then
        echo "[ERROR] No such directory: ${dirPath}"
        return 1
    fi

    checkFilePath=${dirPath}'/'${checkFileName}
    if [ -f ${checkFilePath} ]
    then
        rm ${checkFilePath}
    fi

    # filePaths=$(find ${dirPath} -mindepth 1 -maxdepth 1 -type f -name ${fileNamePattern})
    filePaths=($(ls -l ${dirPath}/${fileNamePattern} 2> /dev/null | awk '/^-/ {print $NF}'))
    if [ ${#filePaths[@]} == 0 ]
    then
        echo "[ERROR] No such files matched pattern: ${fileNamePattern}"
        return 1
    fi

    for path in ${filePaths[@]}
    do
        checkInfo=$(buildCheckInfo ${path}) 
        echo "[INFO] Check info of file ${path}: ${checkInfo}"
        echo ${checkInfo} >> ${checkFilePath}
    done
}


# [Overwrite] 生成文件的的校验信息
# @param $1: 数据文件路径
# @echo 文件校验信息（只能输出此信息）
function buildCheckInfo() {
    echo "OVERWRITE"
}


# 返回文件大小，单位：字节
# @param $1: 数据文件路径
# @echo 文件大小
function getFileSize() {
    echo $(ls -l $1 | awk '{print $5}')
}


# 返回文件行数
# @param $1: 数据文件路径
# @echo 文件行数
function getFileLines() {
    echo $(wc -l $1)
}


# 返回文件创建时间
# @param $1: 数据文件路径
# @echo 文件创建时间
function getFileTime() {
    local cdate=$(ls --full-time $1 | awk '{print $6}'| awk -F- '{print $1$2$3}')
    local ctime=$(ls --full-time $1 | awk '{print $7}'| awk -F. '{print $1}' | awk -F: '{print $1$2$3}')
    echo ${cdate}${ctime}
}


# 解析参数并生成校验文件
function main() {
    parseParams $@
    if [ $? != 0 ];then
        exit 1
    fi

    check
    if [ $? != 0 ];then
        exit 1
    fi
}
