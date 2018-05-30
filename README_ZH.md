# MyHub简介

MyHub是一个由Go开发高性能MySQL代理中间件项目，MyHub在满足基本的读写分离的功能上，致力于简化MySQL分库分表操作。
MyHub和其它数据库中间件相比最大特点是做到最大限度的仿真MySql。

### 基础功能
- 遵守Mysql原生协议，跨语言的通用中间件代理
- 支持透明的MySQL连接池，不必每次新建连接。
- 支持多个slave，slave之间通过权值进行负载均衡。
- 支持读写分离。
- 支持多租户。
- 支持prepare特性。
- 支持到后端DB的最大连接数限制。
- 支持SQL日志及慢日志输出。
- 支持客户端IP白名单。
- 支持字符集设置。
- 支持last_insert_id功能。
- 支持show databases,show tables

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
- 支持主键自增长ID。


# MyHub配置入门

### 配本配置：

启动参数
--cnf 配置文件存放配置文件, 默认'conf/myhub.xml'
如：myhub.exe --cnf conf/myhub.xml

<serveListen>0.0.0.0:8520</serveListen>
MyHub 监听的host和端口


### 日志(log)配置:

<logPath>logs</logPath>
配置路径，默认是Myhub当前目录下的logs目录

<logLevel>warn</logLevel>
日志级别:[debug|info|warn|error]

<logSql>on</logSql>
是否开启sql语句输出[on|off]

<slowLogTime>100</slowLogTime>
开启慢日志（时间单位:毫秒）,默认是0不开启

### 用户(user)配置:

<users>
    <!-- db1,db2,ip1,ip2 * means any database or ip -->
    <user name="root" passwrod="123456" charset="utf-8" db="db1" ip="*"/>
</users>

参数说明：
- 'name' 连接myhub的用户名
- 'passwrod' 连接myhub的密码
- 'charset' 字符集
- 'db' 可使用的逻辑数据库，多个用","分隔，如:'db1,db2'，'*'表示所有逻辑数据库
- 'ip' 允许连接的客户端ip，多个用","分隔，如:'192.168.1.20,192.168.1.30'，'*'表示所有ip

### 逻辑库(schema)配置:

添加两个逻辑数库:db1,test_1;
其中db1中添加了三个逻辑表:dealer_info,cash_record,api_log;

<schema>
    <dataBase name="db1" proxyDataBase="lb_ss">
        <!--  rule: hash | range | date_month | date_day  -->
        <table name="dealer_info" ruleKey="id" rule="rang_1" createSql="dealer_info.sql"/>
        <table name="cash_record" ruleKey="add_time" rule="rang_2" createSql="cash_record.sql"/>
        <table name="api_log" ruleKey="id" rule="hash_1" createSql="api_log.sql"/>
    </dataBase>
    <!-- 直接代理 -->
    <dataBase name="test_1" proxyDataBase="test"/>
</schema>

参数说明：
- 'name' Myhub 的数据库名
- 'proxyDataBase' 代理的节点数据库名
- 'ruleKey'表示表分片所依赖的字段名
- 'rule' 分表表分片规则，参见: <rules><rules>
- 'createSql' 自动创建分表的create 语句