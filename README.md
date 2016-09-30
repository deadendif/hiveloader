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

### TagDetector
##### 功能
检测tag是否（更新包括tag首次生成和tag被重新生成两种情况），用于判定是否触发操作
##### 流程
1. 读取本地tag操作历史，得到该tag对应的上一次操作的历史时间historyTime
2. 扫描本地tag集，找到tag集中近duration天内大于且最接近historyTime的tag生成时间newerTime（即继续上一次的操作）。如果存在这样的newerTime，则检测成功，返回newerTime的值、newerTime所归属的日期（用于计算账单日期，减1天或1月），并触发操作；否则检测失败，不触发操作。

### WebReloader
##### 功能
同步Hive中某天或某月的数据到Oracle（Hive > Local > Oracle，WEB回导）
##### 正常流程：
1. 根据TagDetector的检测结果得到此次操作的操作时间operationTime及账单日期recordDate
2. 从Hive中导出对应账单日期的数据到本地，备份数据，执行SQL清空Oracle对应日期的数据（避免重复导入）
3. 更新tag的历史操作时间为operationTime
4. 使用sqlldr将本地数据导入Oracle（代码中不含此部分功能）

##### 重跑流程
1. 不检测tag，遍历重跑账单日期区间，针对每个日期执行上述正常流程中的步骤2
2. 使用sqlldr将本地数据导入Oracle（代码中不含此部分功能）

### FsReloader
#### 功能
同步Hive中某天或某月的数据到本地（Hive > Local，FTP回导、VGOP回导）
##### 正常流程
1. 根据TagDetector的检测结果得到此次操作的操作时间operationTime及账单日期recordDate
2. 从Hive中导出对应账单日期的数据到本地，切分文件（可选），生成校验文件（可选），备份（可选）
3. 更新tag的历史操作时间为operationTime

##### 重跑流程（待完成）
1. 不检测tag，遍历重跑账单日期区间，针对每个日期执行上述正常流程中的步骤2

#### 潜规则
+ 若数据文件名为hiveloader_2016.txt，则校验文件名为hiveloader_2016.verf，拆分文件名为hiveloader_2016_NNN.txt
