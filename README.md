# hiveloader
增量下载Hive表中的数据到本地

+ Hive > Local > Oracle
+ Hive > Local > FTP


# Install
+ Coreutils版本低于8.16的需手动编译coreutils,详见lib/coreutils/README




# 逻辑

## WebReloader

#### 正常流程：
1. 检测tag近duration天内继上次回导后，是否检测到新生成(新生成包含首次生成和重跑生成)
   若有，则返回近duration天内新生成中最旧的那个tag信息，否则脚本退出
2. 根据tag信息（tag生成时间戳、tag所属日期目录）从Hive中导出数据到本地，同时备份数据，执行SQL
3. 最后更新该tag此次回导操作的历史时间记录

#### 重跑流程


## VgopReloader

# 潜规则
+ 若数据文件名为hiveloader_2016.txt，则校验文件名为hiveloader_2016.verf，拆分文件名为hiveloader_2016_NNN.txt
