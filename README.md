[English](README.md)   [中文](README_ZH.md)

# MyHub Introduction

MyHub is a high-performance MySQL agent middleware project developed by Golang, MyHub is dedicated to simplifying the MySQL sub-segmentation
operation in fulfilling the basic functions of read-write separation.
Compared with other database middleware, MyHub is the most powerful feature to simulate MySql to the maximum extent,
Connecting to Myhub with management tools is like connecting to Mysql.
MyHub can automatically disable the faulty node database, and Myhub can automatically discover and enable the node after
the failed node database is restarted.
Please [get the latest version of the RPM installation package from the release page.](https://github.com/sgoby/myhub/releases)

![SQLyog Screenshots](https://github.com/sgoby/myhub/blob/master/doc/sqlyog.png)

### Basis
- Comply with the Mysql native protocol and support cross-language.
- Support MySQL connection pool, no need to create a new connection each time.
- Support multiple 'Slave', load balancing between 'Slave'。
- Support for reading and writing separation(Need to configure the mysql master-slave data to automatically synchronize).
- Support multi-tenancy。
- Support for 'Prepare' feature。
- Support for maximum connection limit to backend DB.
- Support SQL log and slow log output.
- Support client IP whitelist.
- Support SQL blacklist mechanism.
- Support for charset settings.
- Support last_insert_id。
- Support Mysql command: show databases,show tables.

### Fragmentation

- Support hash and range segmentation by integer.
- Support date-segmentation by year, month, and day.
- Support cross-node sub-tables, sub-tables can be distributed in different nodes.
- Support cross-nodes execute aggregate functions count, sum, max, and min.
- Support the join operation of a single sub-table,it must the join operation of the fragmentation table and
  another single table, and on the same node database.
- Support cross-node: (order by,group by,limit) operations.
- Support for distributed transactions (low XA).
- Support database direct proxy and forwarding.
- Support (insert,delete,update,replace) to multiple nodes table.
- Support automatic creation of sub-tables on multiple nodes.
- Support primary key ID auto increment.

### Install

**RPM install**

- Download & Install

        wget https://github.com/sgoby/myhub/releases/download/0.0.1/myhub-0.0.1-1.x86_64.rpm
        rpm -ivh myhub-0.0.1-1.x86_64.rpm

- Start service

        service myhub start

**Build install**

- Install: golang and git

- Install on Linux (build_linux.sh)

        dir=`pwd`
        git clone https://github.com/sgoby/myhub src/github.com/sgoby/myhub
        export GOPATH=$dir
        echo $GOPATH
        go build -o bin/myhub src/github.com/sgoby/myhub/cmd/myhub/main.go
        echo Congratulations. Build success!

- Install on Windows(build_windows.bat)

        git clone https://github.com/sgoby/myhub src/github.com/sgoby/myhub
        set dir=%cd%
        set GOPATH=%GOPATH%;%dir%
        go build -o bin/myhub.exe src/github.com/sgoby/myhub/cmd/myhub/main.go
        echo Congratulations. Build success!


# MyHub config

### Basis：

Start args
-cnf configuration file path, Default:'conf/myhub.xml'
ex：myhub.exe -cnf conf/myhub.xml

    <serveListen>0.0.0.0:8520</serveListen>
MyHub listener host and port, Default:8520

    <workerProcesses>0</workerProcesses>
The number of worker processes, Default:0 represent use the number of CPU core.

    <maxConnections>2048</maxConnections>
Myhub maximum number of clients connected,Default:2048

### Log:

    <logPath>logs</logPath>
Log output directory. Default:logs

    <logLevel>warn</logLevel>
Log level. value:[debug|info|warn|error] Default:error

    <logSql>on</logSql>
SQL log's switch. value:[on|off] Default:off

    <slowLogTime>100</slowLogTime>
Record SQL, when execute use time more than config time (ms),Default:0 represent turn off slow log

### Users:

    <users>
        <!-- db1,db2,ip1,ip2 * means any database or ip -->
        <user name="root" passwrod="123456" charset="utf-8" db="db1" ip="*"/>
    </users>

**Description:**
- 'name' Represent: client user name of connect Myhub.
- 'passwrod' Represent: client passwrod of connect Myhub.
- 'charset' Represent: client charset of connect Myhub. Default:UTF-8
- 'db' Represent: schema database  the user can use, multiple separated by','. ex:'db1,db2', '*'represent all. Default:*
- 'ip' Represent: allow client's IP to connect to Myhub, support any match chart '\*', multiple separated by','; ex:'192.168.1.20,192.168.2.\*', Default:'127.0.0.1'.


### Nodes:

Add Mysql node host on Myhub.

    <node>
        <hosts>
            <!-- write only(master) --->
            <host type="write" name="host_1" address="127.0.0.1:3306" user="root" password="123456">
                <!-- read only(slave) --->
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

**Description:**
- 'host' Represent: backend mysql host config.
- 'host' -> 'type' Represent: node type. value:[write|read]
- 'host' -> 'name' Represent: backend mysql host name.
- 'host' -> 'address' Represent: backend mysql host address.
- 'host' -> 'user' Represent: backend mysql account user.
- 'host' -> 'password' Represent: backend mysql account password.
- 'host' -> 'weight' Represent: the weight of slave host.
- 'dataBase' Represent: backend mysql database config.
- 'dataBase' -> 'name' Represent: database name.
- 'dataBase' -> 'host' Represent: host name of the node where the database is located, *Link: 'host' -> 'name'*
- 'dataBase' -> 'maxOpenConns' Represent: maximum number of connections in the database, Default:16
- 'dataBase' -> 'maxIdleConns' Represent: database connection maximum idle number, Default:4
- 'dataBase' -> 'maxIdleTime' Represent: database connection maximum idle time(s), Default:60

### Schema:

Add schema database to Myhub.

    <schema>
        <dataBase name="db1" proxyDataBase="lb_ss" blacklistSql="blacklist/db1.sql">
            <!--  rule: hash | range | date_month | date_day  -->
            <table name="dealer_info" ruleKey="id" rule="rang_1" createSql="dealer_info.sql"/>
            <table name="cash_record" ruleKey="add_time" rule="rang_2" createSql="cash_record.sql"/>
            <table name="api_log" ruleKey="id" rule="hash_1" createSql="api_log.sql"/>
        </dataBase>
        <!-- direct proxy -->
        <dataBase name="test_1" proxyDataBase="test"/>
    </schema>

**Description:**
- 'dataBase' Represent: schema database config.
- 'dataBase' -> 'name' Represent: schema database name.
- 'dataBase' -> 'proxyDataBase' Represent: the node database name being proxied, use for direct proxy.
- 'dataBase' -> 'blacklistSql' Represent: SQL blacklist statement, the value can be a SQL file path, or a SQL statement，ex: delete from user where id = ?
- 'table' Represent: schema table config.
- 'table' -> 'name' Represent: table name, must be guaranteed to be unique.
- 'table' -> 'ruleKey' Represent: segmentation rule key, it must be the field name in the table.
- 'table' -> 'rule' Represent: segmentation rule. [Linke:rules](#segmentation-rule)
- 'table' -> 'createSql' Represent: automatically create a create statement for the sub-table, the value can be a SQL file path, or a SQL statement.

### Segmentation rule:

Myhub support three kinds of fragmentation rules:

**1. hash**

This rule uses the modulo operation. This algorithm can divide adjacent data into the same slice according to the 'rowLimit' value in config,
which reduces the difficulty of inserting transaction transaction control.

**2. range**

The algorithm of this rule is to divide the table according to the range of the value of the rule key field.
ex: start <= range < end。

**3. date (Year, Month, Day)**

This rule can be sharded by (year, month, day) and supports multiple date cycles，
ex: rowLimit="7d" represent every 7 days as a shard, start <= date < end.

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

**Description:**
- 'rule' Represent: rule config
- 'name' Represent: rule name, must be guaranteed to be unique.
- 'ruleType' Represent: rule type. value:[range|hash|date]
- 'format' Represent: The suffix format of the sub-table when auto create，

        (1. the 'ruleType' in[range|hash] use format '%d', ex：format="%04d", it will create table whith name tablename_0001;
        (2. the 'ruleType' equal 'date', use format in[y|m|d], it represent[Year|Month|Day]. ex:format="ym" it will create table whith name tablename_201805;
           
- 'maxLen' Represent: modulus value, just configured with ruleType = "hash".
- 'shard' Represent: shard config.
- 'shard' -> 'nodeDataBase' Represent: associated node database name, the value must be in node database config.
- 'shard' -> 'rowLimit' Represent: the number of rows per table is limited, and its meaning is as follows:

        (1. ruleType='range' represent the number of rows per subtable, the value type is a number.
            ex: rowLimit = "100000" represent the maximum number of rows per table is 100000
        (2. ruleType='date' represent the number of rows per time table divided by time, the value is a combination of numbers and [y|m|d].
            ex: rowLimit = "1ym" represents data for one month per sub-segment.
        (3. ruleType='hash' represent each sub-table is divided by the remainder of the modulo, and the value type is a number.
            ex: rowLimit = "2" represents  10 % 0 and 10 % 1 will be saved in the same table

- 'shard' -> 'between' Represent: the range of fragmentation rule values.