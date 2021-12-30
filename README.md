# druid-sql sql解析器

### 介绍
druid-sql 是基于 druid 数据源分离出来的 sql解析器

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
//单 sql 解析
SQLStatement sqlStatement = SQLUtils.parseSingleStatement(sql, DbType.hive);
//多 sql 解析
List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, DbType.hive);
//sql 格式化
String format = SQLUtils.format(sql, DbType.hive);
```

### 支持的数据库
[DbType](https://github.com/ytwp/druid-sql/blob/dba9aabeced9819fc764ec9897a1f720ff173a97/src/main/java/wang/yeting/sql/DbType.java)
