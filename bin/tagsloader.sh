#!/bin/bash

base_dir=$(cd $(dirname $(dirname $0)); pwd)
echo "Hive loader location: "${base_dir}
export PYTHONPATH=${base_dir}
cd ${base_dir}

log_dir=${base_dir}"/logs/tagsLoader/"`date +%Y%m%d`
log_path=${log_dir}"/"`date +%Y%m%d%H%M%S`".log"
if [ ! -d $log_dir ];then
    mkdir -p ${log_dir}
fi
echo "Log location: "${log_path}

script_file=${base_dir}"/src/hdfsTagsLoader/TagsLoader.py"
echo "Begin running tags loader script: "${script_file}
python ${script_file} > ${log_path} &
script_pid=$!

# 脚本执行时长超过10800秒，则认为任务超时，杀死进程
script_life_ticks=10800
while [ $script_life_ticks -gt  0 ]
do
    pid_count=`ps -p ${script_pid} | wc -l`
    if [ $pid_count -eq 1 ];then  #后台进程已经退出
        echo "--------- [begin] script log ------------"
        cat ${log_path}
        echo "--------- [end] script log ------------"
        error_info=`grep ERROR: ${log_path} | wc -l`
        if [ ${error_info} -gt 0 ];then
            echo "ERROR: ${script_file} has exit failed!"
            exit -1
        else
            echo "${script_file} has exit success!"
            exit 0  
        fi
    fi
    sleep 1
    script_life_ticks=$(( $script_life_ticks - 1 ))
done

echo "--------- [begin] script log ------------"
cat ${log_path}
echo "--------- [end] script log ------------"
# 杀死该进程
kill -9 ${script_pid}
echo "ERROR: ${script_file} run timeout and it has been killed!"
exit -1
