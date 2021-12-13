package wang.yeting.sql;

import wang.yeting.sql.ast.SQLStatement;

/**
 * @author : weipeng
 * @since : 2021-12-01 6:32 下午
 */

public class Test {

    public static void main(String[] args) {
        new Test().testSqlParser();
    }

    public void testSqlParser() {
        String sql = "select * from test;";
        String dbType = "hive";
        System.out.println("原始SQL 为 ： " + sql);
        String result = SQLUtils.format(sql, dbType);
        System.out.println(result);
        SQLStatement statement = SQLUtils.parseSingleStatement(sql, dbType);
        System.out.println(statement.toString());
    }
}
