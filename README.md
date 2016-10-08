## hiveloader
增量下载Hive表中的数据到本地

## 安装
+ Coreutils版本低于8.16的需手动编译coreutils,详见lib/coreutils/README

## 说明
### TagsLoader
##### 功能 
将HDFS上的tag集同步到本地
##### 流程
1. 从HDFS上下载近duration天内的全部tag及其生成时间，存放在本地按日期归档，文件名为tag，文件内容为tag生成的时间戳（毫秒）
##### 用法
```
bin/tagsloader.sh
```

### TagDetector
##### 功能
检测tag是否（更新包括tag首次生成和tag被重新生成两种情况），用于判定是否触发操作
##### 流程
1. 读取本地tag操作历史，得到该tag对应的上一次操作的历史时间historyTime
2. 扫描本地tag集，找到tag集中近duration天内大于且最接近historyTime的tag生成时间newerTime（即继续上一次的操作）。如果存在这样的newerTime，则检测成功，返回newerTime的值、newerTime所归属的日期（用于计算账单日期，减1天或1月），并触发操作；否则检测失败，不触发操作。
##### 用法
```
bin/tagdetector.sh <tag> <historyPath> [<duration>]
```
+ `tag`：待检测的tag
+ `historyPath`：用于与tag集对比的历史路径
+ `duration`：检测时间范围，单位：天

示例  
```
bin/tagdetector.sh  '10000' 'data/webReloaderTagsHistory'
bin/tagdetector.sh  '10000' 'data/webReloaderTagsHistory' '4'
```


### WebReloader
##### 功能
同步Hive中某天或某月的数据到Oracle（Hive > Local > Oracle）
##### 正常流程：
1. 根据TagDetector的检测结果得到此次操作的操作时间operationTime及账单日期recordDate
2. 从Hive中导出对应账单日期的数据到本地，备份数据，执行SQL清空Oracle对应日期的数据（避免重复导入）
3. 更新tag的历史操作时间为operationTime
4. 使用sqlldr将本地数据导入Oracle（代码中不含此部分功能）

##### 重跑流程
1. 不检测tag，遍历重跑账单日期区间，针对每个日期执行上述正常流程中的步骤2
2. 使用sqlldr将本地数据导入Oracle（代码中不含此部分功能）

##### 用法
```
# 正常流程
bin/webreloader.sh <tag> <tableList> <hqlList> <sqlList> <type>

# 重跑流程
bin/webreloader.sh <tag> <tableList> <hqlList> <sqlList> <startDate> <endDate>
```
+ `tag`：触发web回导的tag
+ `tableList`：待导入的Oracle表列表，Oracle表格式：<用户名>/<密码>@<实例名>:<表名>，多个Oracle表用`&`分隔
+ `hqlList`：从Hive上查询数据的HQL列表，HQL格式：**日期字段值用`%s`占位**，多个HQL用`&`分隔
+ `sqlList`：清空Oracle表的SQL列表，SQL格式：**日期字段用`%s`占位**，多个SQL用`&`分隔
+ `type`：任务周期类型，取值`DAY`、`MONTH`
+ `startDate`：重跑的起始日期，如20160910或201609
+ `endDate`：重跑的结束日期（包含），如20161011或201610，当`endDate`与`startDate`相同时，表示重跑一天的数据

示例  
```
# 单表正常流程
bin/webreloader.sh '10000' 'user/password@DB:USER.TEST_TABLE' 'SELECT IP, OS_VERSION, IMEI, DAY_ID FROM AMAPP_SDK_PEXG_LOGIN WHERE DAY_ID = %s' 'DELETE FROM USER.TEST_TABLE WHERE DAY_ID = %s' 'DAY'

# 多表正常流程
bin/webreloader.sh '10000' 'user/password@DB:USER.TEST_TABLE&user2/password2@DB2:USER2.TEST_TABLE2' 'SELECT IP, OS_VERSION, IMEI, DAY_ID FROM AMAPP_SDK_PEXG_LOGIN WHERE DAY_ID = %s&SELECT IP, DAY_ID FROM AMAPP_SDK_PEXG_LOGIN2 WHERE DAY_ID = %s' 'DELETE FROM USER.TEST_TABLE WHERE DAY_ID = %s&DELETE FROM USER2.TEST_TABLE2 WHERE DAY_ID = %s' 'DAY'

# 重跑流程
bin/webreloader.sh '10000' 'user/password@DB:USER.TEST_TABLE' 'SELECT IP, OS_VERSION, IMEI, DAY_ID FROM AMAPP_SDK_PEXG_LOGIN WHERE DAY_ID = %s' 'DELETE FROM USER.TEST_TABLE WHERE DAY_ID = %s' '20160918' '20160919'
```

### FsReloader
#### 功能
同步Hive中某天或某月的数据到本地（Hive > Local）
##### 正常流程
1. 根据TagDetector的检测结果得到此次操作的操作时间operationTime及账单日期recordDate
2. 从Hive中导出对应账单日期的数据到本地，切分文件（可选），生成校验文件（可选），备份（可选）
3. 更新tag的历史操作时间为operationTime

##### 重跑流程（dai）
1. 不检测tag，遍历重跑账单日期区间，针对每个日期执行上述正常流程中的步骤2

##### 潜规则
+ 若数据文件名为hiveloader_2016.txt，则校验文件名为hiveloader_2016.verf，拆分文件名为hiveloader_2016_NNN.txt

##### 用法
```
# 正常流程
bin/vgopreloader.sh <tag> <hqlList> <dirNameList> <dataFileNameList> <maxFileSize> <serialNoWidth> <checkerName> <checkerFieldSeparator> <type>
```
+ `tag`：触发web回导的tag
+ `hqlList`：从Hive上查询数据的HQL列表，HQL格式：日期字段用`%s`占位，多个HQL用`&`分隔
+ `dirNameList`：存放数据的目录名，多个目录名用`&`分隔
+ `dataFileNameList`：数据文件名，按照上述*潜规则*生成校验文件名和拆分文件名，**日期用`%s`占位**，多个文件名用`&`分隔
+ `maxFileSize`：拆分后文件大小的最大值，**当为0时，不拆分文件**，单位：字节
+ `serialNoWidth`：拆分文件后编号位数，当`maxFileSize > 0`时有效
+ `checkerName`：用于生成校验文件的脚本名，脚本需位于`lib/checkers`，自定义脚本规则见`lib/checkers/README`，**当为空字符串`''`时，不生成校验文件**
+ `checkerFieldSeparator`：校验文件中字段的分隔符，当`checkerName`不为空字符串时有效
+ `type`：任务周期类型，取值`DAY`、`MONTH`

示例  
```
# 正常流程
bin/vgopreloader.sh '10000' 'SELECT ID, NAME, DAY FROM ZZC.HIVE_DATA_D WHERE DAY = %s' 'TEST_HIVE_D' 'FILENAME_D_%s_00.dat' '6553600' '5' 'vgopChecker.sh' '$' 'DAY'
```