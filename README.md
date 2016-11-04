## hiveloader
增量下载Hive表中的数据到本地

## 安装
+ Coreutils版本低于8.16的需手动编译coreutils,详见`lib/coreutils/README`

## 说明
### 代码层次结构图
!["代码层次结构图"](https://www.deadend.me/media/images/2016/10/17/6940a68b-81b7-4564-a212-3383622da31c.png)

+ `bin`层：负责环境初始化及`scripts`层的脚本的调用
+ `scripts`层：负责参数校验、解析，并调用`src`层代码
+ `src`层：实现了tag下载、检测，利用`mixin`机制实现了各Reloader类


### TagsLoader
##### 功能 
将HDFS上的tag集同步到本地
##### 流程
1. 从HDFS上下载近duration天内的全部tag及其生成时间，存放在本地按日期归档，文件名为tag，文件内容为tag生成的时间戳（毫秒）
##### 用法
```bash
bin/tagsloader.sh
```

### TagDetector
##### 功能
检测tag是否有新的有效生成（包括tag首次生成和tag被重新生成两种情况），用于判定是否触发操作

##### 流程
1. 读取本地tag操作历史，得到该tag对应的上一次操作的历史时间historyTime
2. 扫描本地tag集，找到tag集中近duration天内大于且最接近historyTime的tag生成时间newerTime（即继续上一次的操作）。如果存在这样的newerTime，则检测成功，返回newerTime的值、newerTime所归属的日期（用于计算账单日期，减1天或1月），并触发操作；否则检测失败，不触发操作。

##### 用法
```bash
bin/tagdetector.sh <tag> <historyPath> [<duration>]
```
+ `tag`：待检测的tag
+ `historyPath`：用于与tag集对比的历史路径
+ `duration`：检测时间范围，单位：天

示例  
```bash
bin/tagdetector.sh  '10000' 'data/webReloaderTagsHistory'
bin/tagdetector.sh  '10000' 'data/webReloaderTagsHistory' '4'
```

### WebReloader
##### 功能
同步Hive中某些天或某些月或全部的数据到Oracle（Hive > Local > Oracle）

##### 正常流程：
1. 根据TagDetector的检测结果得到此次操作的操作时间operationTime及待同步的账单日期
2. 依次从Hive中导出对应账单日期的数据到本地，备份数据，执行SQL清空Oracle对应日期的数据（避免重复导入）
3. 更新tag的历史操作时间为operationTime
4. 使用sqlldr将本地数据导入Oracle（代码中不含此部分功能）

##### 重跑流程
1. 不检测tag，遍历重跑账单日期区间，针对每个日期执行上述正常流程中的步骤2
2. 使用sqlldr将本地数据导入Oracle（代码中不含此部分功能）

##### 用法
```bash
# 正常流程
bin/webreloader.sh <tag> <tableList> <hqlList> <sqlList> <type> <deltaList>

# 重跑流程
bin/webreloader.sh <tag> <tableList> <hqlList> <sqlList> <startDate> <endDate>
```
+ `tag`：触发web回导的tag
+ `tableList`：待导入的Oracle表列表，Oracle表格式：<用户名>/<密码>@<实例名>:<表名>，多个Oracle表用`&`分隔
+ `hqlList`：从Hive上查询数据的HQL列表，HQL格式：**如果是增量查询，日期字段值用`%s`占位**，多个HQL用`&`分隔
+ `sqlList`：清空Oracle表的SQL列表，SQL格式：**如果是增量删除，日期字段用`%s`占位**，多个SQL用`&`分隔
+ `type`：任务周期类型，取值`DAY`、`MONTH`
+ `deltaList`：账单日期与tag归档日期的差值，举例：`-1`表示前1天/月，`-1,-3,-5`（或`-5，-3，-1`）表示前1、3、5天/月，`-1#-4`（或`-4#-1`）表示前1、2、3、4天/月，多个差值的顺序与回导的顺序对应
+ `startDate`：重跑的起始日期，如20160910或201609
+ `endDate`：重跑的结束日期（包含），如20161011或201610，当`endDate`与`startDate`相同时，表示重跑一天的数据

示例  
```bash
# 正常流程（单表）
bin/webreloader.sh '10000' 'ora_user/password@DB:ORA_USER.TEST_TABLE' 'SELECT IP, OS_VERSION, IMEI, DAY_ID FROM HIVE_USER.AMAPP_SDK_PEXG_LOGIN WHERE DAY_ID = %s' 'DELETE FROM ORA_USER.TEST_TABLE WHERE DAY_ID = %s' 'DAY' '-1'

# 正常流程（全量）
bin/webreloader.sh '10000' 'ora_user/password@DB:ORA_USER.TEST_TABLE' 'SELECT IP, OS_VERSION, IMEI, DAY_ID FROM HIVE_USER.AMAPP_SDK_PEXG_LOGIN WHERE DAY_ID = %s' 'DELETE FROM ORA_USER.TEST_TABLE WHERE DAY_ID = %s' 'DAY' '-1'

# 正常流程（多表）
bin/webreloader.sh '10000' 'ora_user/password@DB:ORA_USER.TEST_TABLE&ora_user2/password2@DB2:ORA_USER2.TEST_TABLE2' 'SELECT IP, OS_VERSION, IMEI, DAY_ID FROM HIVE_USER.AMAPP_SDK_PEXG_LOGIN WHERE DAY_ID = %s&SELECT IP, DAY_ID FROM HIVE_USER.AMAPP_SDK_PEXG_LOGIN2 WHERE DAY_ID = %s' 'DELETE FROM ORA_USER.TEST_TABLE WHERE DAY_ID = %s&DELETE FROM ORA_USER2.TEST_TABLE2 WHERE DAY_ID = %s' 'DAY' '-1,-3'

# 重跑流程
bin/webreloader.sh '10000' 'ora_user/password@DB:ORA_USER.TEST_TABLE' 'SELECT IP, OS_VERSION, IMEI, DAY_ID FROM HIVE_USER.AMAPP_SDK_PEXG_LOGIN WHERE DAY_ID = %s' 'DELETE FROM ORA_USER.TEST_TABLE WHERE DAY_ID = %s' '20160918' '20160919'
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
```bash
# 正常流程
bin/vgopreloader.sh <tag> <hqlList> <dirNameList> <dataFileNameList> <maxFileSize> <serialNoWidth> <checkerName> <checkerFieldSeparator> <type>

# 重跑流程
bin/vgopreloader.sh <tag> <hqlList> <dirNameList> <dataFileNameList> <maxFileSize> <serialNoWidth> <checkerName> <checkerFieldSeparator> <startDate> <endDate>
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
+ `startDate`：重跑的起始日期，如20160910或201609
+ `endDate`：重跑的结束日期（包含），如20161011或201610，当`endDate`与`startDate`相同时，表示重跑一天的数据

示例  
```bash
# 正常流程（单HQL）
bin/vgopreloader.sh '10000' 'SELECT ID, NAME, DAY FROM ZZC.HIVE_DATA_D WHERE DAY = %s' 'TEST_HIVE_D' 'FILENAME_D_%s_00.dat' '6553600' '5' 'vgopChecker.sh' '$' 'DAY'

# 正常流程（多HQL）
bin/vgopreloader.sh '10000' 'SELECT ID, NAME, DAY FROM ZZC.HIVE_DATA_D WHERE DAY = %s&SELECT ID, DAY FROM ZZC.HIVE_DATA_D2 WHERE DAY = %s' 'TEST_HIVE_D&TEST_HIVE_D2' 'FILENAME_D_%s_00.dat&FILENAME_D2_%s_00.dat' '6553600' '5' 'vgopChecker.sh' '$' 'DAY'

# 重跑流程
bin/vgopreloader.sh '10000' 'SELECT ID, NAME, DAY FROM ZZC.HIVE_DATA_D WHERE DAY = %s' 'TEST_HIVE_D' 'FILENAME_D_%s_00.dat' '6553600' '5' 'vgopChecker.sh' '$' '20160918' '20160919'
```

## 程序扩展
### VgopRealoder文件校验方式扩展
配置文件`etc/hiveloader.ini`中配置项`checkers.path`是校验脚本存放的路径。按照`lib/checkers/README`规范编写脚本（设脚本名为`demo.sh`）并把脚本放在`checkers.path`对应的目录。然后将脚本调用参数中的`checkerName`修改为新脚本名（`demo.sh`）即可。


### Hive数据下载方式扩展
Hive数据的下载方式因集群环境不同，可能需要进行扩展。扩展方法（参考`JavaLoaderMixin`或`ShellLoaderMixin`类）：

+ 编写`DemoLoaderMixin`类，继承`AbstractLoaderMixin`类并实现（重写）父类的`_load()`方法，该方法定义如下

    ```python
    """
    [Overwrite] 执行命令从Hive上下载数据
    @param hql: 从Hive下载数据执行的HQL
    @param loadPath: 下载路径
    @param fileName: 数据文件名
    @return 是否下载成功
    """
    def _load(self, hql, loadPath, fileName):
        return True
    ```
+ 在配置文件`etc/hiveloader.ini`的`coreHiveLoader`配置块中，修改对应的`reloader.base.loader`的值，该值使子类（`JavaLoaderMixin`或`ShellLoaderMixin`）动态加载父类。

### Hive数据下载及处理流程扩展
+ 采用`mixin`机制实现`XxxReloader`类（参考`FsReloader`或`WebReloader`类），并重写动态加载的父类`DemoLoaderMixin`的`_run()`方法，方法定义如下：

    ```
    """
    [Overwrite] 执行第i个子操作
    @param i: 下标
    @return 是否执行成功
    """
    def _run(self, i):
        return True
    ```

### 程序参数解析扩展
+ 在`scripts`层编写`valitdate.py`和`xxxReload.py`脚本，前者实现参数合法性的校验，后者实现参数的解析及`XxxReloader`的调用
+ 在`bin`层编写`xxxreloader.sh`脚本，实现环境的初始化及`xxxReload.py`的调用
