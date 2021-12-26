# druid-sql sql解析器

### 介绍
druid-sql 是基于 druid 数据源分离出来的sql解析器

### 开源许可协议
本项目基于 [ApacheLicense-2.0](http://www.apache.org/licenses/LICENSE-2.0.txt) 开源许可协议.

### 使用
```xml
<dependency>
    <groupId>wang.yeting</groupId>
    <artifactId>druid-sql</artifactId>
    <version>1.0.0</version>
</dependency>
```
```java
//sql 解析
SQLStatement sqlStatement = SQLUtils.parseSingleStatement(sql, DbType.hive);
//sql 格式化
String format = SQLUtils.format(sql, DbType.hive);
```
