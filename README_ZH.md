[English](README.md)   [中文](README_ZH.md)

# MyHub简介

MyHub是一个由Go开发高性能MySQL代理中间件项目，MyHub在满足基本的读写分离的功能上，致力于简化MySQL分库分表操作。
MyHub和其它数据库中间件相比最大特点是做到最大限度的仿真MySql，用管理工具连接到Myhub就如同连接接Mysql一样。
MyHub能自动停用故障节点数据库，在故障节点数据库恢重启后，Myhub能自动发现并启用节点。
请从[release页面获取最新版的RPM安装包](https://github.com/sgoby/myhub/releases)

![SQLyog 截图](https://raw.githubusercontent.com/sgoby/myhub/master/doc/sqlyog.png)

### 基础功能
- 遵守Mysql原生协议，跨语言的通用中间件代理。
- 支持透明的MySQL连接池，不必每次新建连接。
- 支持多个slave，slave之间通过权值进行负载均衡。
- 支持读写分离(需自行配置mysql端主从的数据自动同步，Myhub不负责任何的数据同步问题)。
- 支持多租户。
- 支持prepare特性。
- 支持到后端DB的最大连接数限制。
- 支持SQL日志及慢日志输出。
- 支持客户端IP白名单。
- 支持SQL黑名单机制。
- 支持字符集设置。
- 支持last_insert_id功能。
- 支持show databases,show tables。

### 分片功能

- 支持按整数的hash和range分表方式。
- 支持按年、月、日维度的时间分表方式。
- 支持跨节点分表，子表可以分布在不同的节点。
- 支持跨节点的count,sum,max和min等聚合函数。
- 支持单个分表的join操作，即支持分表和另一张不分表的join操作。
- 支持跨节点的order by,group by,limit等操作。
- 支持分布式事务（弱XA）。
- 支持数据库直接代理转发。
- 支持（insert,delete,update,replace）到多个node上的子表。
- 支持自动在多个node上创建分表。
- 支持主键自增长ID, 可以支持Twitter's Snowflake分布式ID,只需把自增长字段类型改为bigint。

### 安装

**RPM安装**

- 下载、安装

        wget https://github.com/sgoby/myhub/releases/download/0.0.1/myhub-0.0.1-1.x86_64.rpm
        rpm -ivh myhub-0.0.1-1.x86_64.rpm

- 启动

        service myhub start

**编译安装**

- 安装Golang、git

- Linux 上安装(build_linux.sh)

        dir=`pwd`
        git clone https://github.com/sgoby/myhub src/github.com/sgoby/myhub
        export GOPATH=$dir
        echo $GOPATH
        go build -o bin/myhub src/github.com/sgoby/myhub/cmd/myhub/main.go
        echo Congratulations. Build success!

- Windows 上安装(build_windows.bat)

        git clone https://github.com/sgoby/myhub src/github.com/sgoby/myhub
        set dir=%cd%
        set GOPATH=%GOPATH%;%dir%
        go build -o bin/myhub.exe src/github.com/sgoby/myhub/cmd/myhub/main.go
        echo Congratulations. Build success!


# MyHub配置入门

### 配本配置：

启动参数
--cnf 配置文件存放配置文件, 默认'conf/myhub.xml'
如：myhub.exe --cnf conf/myhub.xml
```xml
<serveListen>0.0.0.0:8520</serveListen>
```
MyHub 监听的host和端口,默认端口:8520
```xml
<workerProcesses>0</workerProcesses>
```
工作进程数,默认是0,表示取当前主机的CPU核心数
```xml
<maxConnections>2048</maxConnections>
```
最大连接数,默认是2048

### 日志(log)配置:
```xml
<logPath>logs</logPath>
```
配置路径，默认是Myhub当前目录下的logs目录
```xml
<logLevel>warn</logLevel>
```
日志级别:[debug|info|warn|error] 默认error
```xml
<logSql>on</logSql>
```
是否开启sql语句输出[on|off] 默认off
```xml
<slowLogTime>100</slowLogTime>
```
开启慢日志（时间单位:毫秒）,默认是0不开启

### 用户(user)配置:
```xml
<users>
    <!-- db1,db2,ip1,ip2 * means any database or ip -->
    <user name="root" passwrod="123456" charset="utf-8" db="db1" ip="*"/>
</users>
```
参数说明：
- 'name' 连接Myhub的用户名
- 'passwrod' 连接Myhub的密码
- 'charset' 字符集
- 'db' 可使用的逻辑数据库，多个用','分隔，如:'db1,db2'，'*'表示所有逻辑数据库
- 'ip' 允许连接的客户端ip支持模糊匹配，'\*'表示所有ip；多个用','分隔；默认'127.0.0.1'；如:'192.168.1.20,192.168.2.\*'。


### 节点(node)配置:

添加两个节点主机：host_1，host_2
```xml
<node>
    <hosts>
        <!-- write only(master) 写库(主库) --->
        <host type="write" name="host_1" address="127.0.0.1:3306" user="root" password="123456">
            <!-- read only(slave) 只读库(从库) --->
            <!-- <host type="read" name="host_1_1"  address = "192.168.31.231:3306" user = "root" password = "123456" weight="1"/> -->
        </host>
        <host type="write" name="host_2" address="192.168.31.231:3306" user="root" password="123456"/>
    </hosts>
    <dataBases>
        <dataBase name="test" host="host_1" maxOpenConns="16" maxIdleConns="4" maxIdleTime="30"/>
        <dataBase name="shard_01" host="host_1" maxOpenConns="16" maxIdleConns="4" maxIdleTime="30"/>
        <dataBase name="shard_02" host="host_1" maxOpenConns="16" maxIdleConns="4" maxIdleTime="30"/>
        <dataBase name="lb_ss" host="host_1" maxOpenConns="16" maxIdleConns="4" maxIdleTime="30"/>
        <dataBase name="lb_livegame" host="host_2" maxOpenConns="16" maxIdleConns="4" maxIdleTime="30"/>
    </dataBases>
</node>
```
参数说明：
- 'host' 节点主机
- 'host' -> 'type' 读写库类型，值[write|read]
- 'host' -> 'name' 节点名称
- 'host' -> 'address' 主机mysql地址
- 'host' -> 'user' 主机mysql登录用户名
- 'host' -> 'password' 主机mysql登录密码
- 'host' -> 'weight' 从库（读库）权值
- 'dataBase' 节点主机数据库
- 'dataBase' -> 'name' 节点上的数据库名称
- 'dataBase' -> 'host' 节点主机名称，对应：'host' -> 'name'
- 'dataBase' -> 'maxOpenConns' 数据库最大连接数，默认:16
- 'dataBase' -> 'maxIdleConns' 数据库连接最大空闲数，默认:4
- 'dataBase' -> 'maxIdleTime' 数据库连接最大空闲时间(时间单位：秒)，默认:60

### 逻辑库(schema)配置:

添加两个逻辑数库:db1,test_1;
其中db1中添加了三个逻辑表:dealer_info,cash_record,api_log;
```xml
<schema>
    <dataBase name="db1" proxyDataBase="lb_ss" blacklistSql="blacklist/db1.sql">
        <!--  rule: hash | range | date_month | date_day  -->
        <table name="dealer_info" ruleKey="id" rule="rang_1" createSql="dealer_info.sql"/>
        <table name="cash_record" ruleKey="add_time" rule="rang_2" createSql="cash_record.sql"/>
        <table name="api_log" ruleKey="id" rule="hash_1" createSql="api_log.sql"/>
    </dataBase>
    <!-- 直接代理 -->
    <dataBase name="test_1" proxyDataBase="test"/>
</schema>
```
参数说明：
- 'dataBase' 逻辑数据库
- 'dataBase' -> 'name' Myhub 的数据库名(必须唯一)
- 'dataBase' -> 'proxyDataBase' 代理的节点数据库名
- 'dataBase' -> 'blacklistSql' SQL黑名单语句，多个用";"分隔，"?"表示通配符，值是可以是SQL文件路径，也可以是SQL语句，ex: delete from user where id = ?
- 'table' 逻辑表
- 'table' -> 'name' 表名称(必须唯一)
- 'table' -> 'ruleKey'表示表分片所依赖的字段名
- 'table' -> 'rule' 分表表分片规则，参见: rules
- 'table' -> 'createSql' 自动创建分表的create 语句，值是可以是SQL文件路径，也可以是SQL语句

### 分片规则(rule)配置:

Myhub 目前支持三种分片规则:
1. 固定分片(hash)

此规则运用求模运算，此算可以法根据rowLimit参数把相邻的数据分到同一分片，减少插入事务事务控制难度。

2. 范围约定(range)

此分片适用于，提前规划好分片字段某个范围属于哪个分片，start <= range < end。

3. 按日期（年、月、日）分片(date)

此规则可以按（年、月、日）分片，支持多个日期周期，如: 可以把每7天(rowLimit="7d")作为的一个分片，其它同理，start <= date < end。
```xml
<rules>
    <rule name="rang_1" ruleType="range" format="%04d">
        <!-- tableRowLimit : 2d,m,y,h-->
        <shard nodeDataBase="test" rowLimit="10000" between="1-8" />
        <shard nodeDataBase="shard_01" rowLimit="10000" between="8-10" />
    </rule>
    <rule name="rang_2" ruleType="date" format="ym">
        <!-- tableRowLimit : 2d,m,y,h-->
        <shard nodeDataBase="test" rowLimit="1m" between="201801-201901" />
    </rule>
    <!-- 'maxLen' represents the count of hash shard total, default 1024 -->
    <rule name="hash_1" ruleType="hash" format="%04d"  maxLen = "12">
        <!-- 'rowLimit' represents every shard table continuous rows count split by 'ruleKey', default 1;
             'between' represents the hash mod value range. ex:'between="0-3",ruleKey's value is 10,
             and 'maxlen'= 10, 10%3 = 1,it menas in the between  0-3 -->
        <shard nodeDataBase="test" rowLimit="2" between="0-6" />
        <shard nodeDataBase="shard_01" rowLimit="2" between="6-12" />
    </rule>
</rules>
```
参数说明：
- 'rule' 规则
- 'name' (必需) 规则名称，在逻辑库表配置会用到
- 'ruleType' (必需) 分片规则[range|hash|date]
- 'format' (可选) 自动创建分表的后缀名，

        (1. 如果分片规则是range|hash 格式为%d 如：format="%04d" 生成的表名是 table_0001;
        (2. 如果分片规则是date 格式为[y|m|d] 分别表示年/月/日 可以是组合 如：format="ym" 生成的表名是 table_201805;

- 'maxLen' (可选) 仅在hash 规则中有用, hash 取模中的被模数
- 'shard' 规则分片
- 'shard' -> 'nodeDataBase' 节点数据库名称，对应节点配置中的 'dataBase' -> 'name'
- 'shard' -> 'rowLimit' 每个分表的行数限制，具体对每个分片规则其含义如下：

        (1. range 规则表示每个分表的行数，值类型为数字 如：rowLimit = "100000" 表示每个分表最大行数为100000 条
        (2. date 规则表示每个分表按时间划分的行数，值为数字和[y|m|d]组合 如：rowLimit = "1ym" 表示每个分表存的数据是一个月
        (3. hash 规则表示每个分表按hash取模的余数，值类型为数字 如：rowLimit = "2"  10 % 0 和 10 % 1 是存在同一个表中

- 'shard' -> 'between' 分片在节点数据的限制范围