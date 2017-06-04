
## SQLonStorm 0.2
SQLonStorm 是一个基于Storm1.1.2 的流数据查询引擎。类似于Hive能够为Hadoop提供SQL语法查询支持，SQLonStorm 的最大特点也是支持SQL语法对Storm上的流数据进行查询。
  
SQLonStorm 目前版本可以实现简单的选择（Selection）、映射（Projection）、基于时间滚动窗口的连接(Join),基于时间滚动窗口的分组(Group-by)以及聚合（Aggregation)操作。五种操作的支持范围请见示例介绍。
   注意：
   
  - SQLonStorm 目前版本仅支持Mysql 作为数据输入源。
  - SQLonStorm 目前版本仅支持整数类型。

---

#### 示例:  
- 选择（Selection) 广泛支持各种包含and、or和括号的条件表达式。不限选择条件的个数。支持>,<,=,!= 四种比较操作符。

``` sql
SELECT * FROM JData_Action_201602 where JData_Action_201602.sku_id=138778 and JData_Action_201602.type=1;

SELECT JData_Action_201602.sku_id, JData_Action_201602.cate FROM JData_Action_201602 where JData_Action_201602.brand>100 or (JData_Action_201602.cate >4 AND JData_Action_201602.type=1);

SELECT * FROM JData_Action_201602 where JData_Action_201602.cate=8 and (JData_Action_201602.type=1 or JData_Action_201602.type=2);
```
- 映射（Projection）支持 *，以"表名.列属性名"为格式的列名和 max、count()两种聚合函数的映射。

``` sql
SELECT JData_Action_201602.sku_id, JData_Action_201602.cate FROM JData_Action_201602

SELECT * FROM JData_Action_201602 where JData_Action_201602.sku_id=138778 and JData_Action_201602.type=6 or JData_Action_201602.type=1 and JData_Action_201602.user_id=200719;

select   JData_Action_201602.sku_id, count(JData_Action_201602.type) as bc from JData_Action_201602  group by JData_Action_201602.sku_id;

select JData_Action_201602.user_id, max(JData_Action_201602.brand) as bc from JData_Action_201602  group by JData_Action_201602.user_id having max(JData_Action_201602.brand) > 200;
``` 

- 连接(Join) Join连接操作目前仅支持两个表之间的连接。它支持inner,left,right 三种join。此外，它基于Storm的TumblingWindow时间滚动窗口实现，默认为将6秒内的流数据进行连接。
``` 
select JData_Action_201602.user_id,max(JData_Product.attr2) FROM JData_Action_201602 INNER JOIN JData_Product ON JData_Action_201602.cate = JData_Product.cate group by JData_Action_201602.user_id;
select * FROM JData_Action_201602 left JOIN JData_Product ON JData_Action_201602.cate = JData_Product.cate

select * FROM JData_Action_201602 right JOIN JData_Product ON JData_Action_201602.cate = JData_Product.cate
``` 

- 分组 基于时间滚动窗口的分组(Group-by) SQL分组语法基于基于Storm的TumblingWindow时间滚动窗口实现，默认将2秒内的流数据进行分组。可支持多个grouby 条件。

``` 

select JData_Action_201602.user_id FROM JData_Action_201602 INNER JOIN JData_Product ON JData_Action_201602.cate = JData_Product.cate group by JData_Action_201602.user_id;
``` 


- 聚合（Aggregation)操作。 SQLonStorm目前版本仅支持max、count 两种聚合条件。支持>,<,=,!= 四种比较操作符。

``` 

select JData_Action_201602.user_id, max(JData_Action_201602.brand) as bc from JData_Action_201602  group by JData_Action_201602.user_id having max(JData_Action_201602.brand) > 200;
select JData_Action_201602.type, count(JData_Action_201602.sku_id) as bc from JData_Action_201602  group by JData_Action_201602.type having count(JData_Action_201602.sku_id) > 5;
``` 




### 原理说明

 实现原理如下图：
    <img src=http://7xtc7i.com1.z0.glb.clouddn.com/Snip20170517_1.png  />



### 下载安装
1. git clone
```
git clone https://github.com/bigdataFlySQL/SQLonStorm.git
```
2. 在/java/domain 文件夹下，新建一个ProjectConfig.java 文件，为SQLonStorm 的系统配置文件。
  示例如下图，请根据各自电脑的系统环境填写好相关的信息：
<img src=http://7xtc7i.com1.z0.glb.clouddn.com/Snip20170604_4.png  />

3. 利用/resources/createtabledata.txt 在Mysql 创建好相关的数据库表，数据自行添加。

4. 在//resources/InputSQL.txt 中填写好需要查询的SQL 语句，注意目前只能填写一句。

5. 执行/java/topology/TopologyMain.java 中的Main函数，在result.txt 中可查看到SQL语句输出结果。



###使用方法
1. 修改join 连接操作的时间段长度。在 /java/topology/TopologyMain.java 中代码片段
    
    <img src=http://7xtc7i.com1.z0.glb.clouddn.com/join.png />
    
    修改对应的时间段数值。
2. group-by 分组操作的时间段长度。在 /java/topology/TopologyMain.java 中代码片段
    
    <img src=http://7xtc7i.com1.z0.glb.clouddn.com/group.png />
    
    修改对应的时间段数值。
 
 



## License
Apache-2.0