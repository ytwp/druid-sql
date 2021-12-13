/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wang.yeting.sql.util;

import wang.yeting.sql.DbType;
import wang.yeting.sql.SQLUtils;
import wang.yeting.sql.ast.statement.SQLCreateTableStatement;
import wang.yeting.sql.logging.Log;
import wang.yeting.sql.logging.LogFactory;

import java.sql.*;
import java.util.*;

public class OracleUtils {

    private final static Log LOG = LogFactory.getLog(OracleUtils.class);

    private static Set<String> builtinFunctions;
    private static Set<String> builtinTables;
    private static Set<String> keywords;

    public static boolean isBuiltinFunction(String function) {
        if (function == null) {
            return false;
        }

        String function_lower = function.toLowerCase();

        Set<String> functions = builtinFunctions;

        if (functions == null) {
            functions = new HashSet<String>();
            Utils.loadFromFile("META-INF/druid/parser/oracle/builtin_functions", functions);
            builtinFunctions = functions;
        }

        return functions.contains(function_lower);
    }

    public static boolean isBuiltinTable(String table) {
        if (table == null) {
            return false;
        }

        String table_lower = table.toLowerCase();

        Set<String> tables = builtinTables;

        if (tables == null) {
            tables = new HashSet<String>();
            Utils.loadFromFile("META-INF/druid/parser/oracle/builtin_tables", tables);
            builtinTables = tables;
        }

        return tables.contains(table_lower);
    }

    public static boolean isKeyword(String name) {
        if (name == null) {
            return false;
        }

        String name_lower = name.toLowerCase();

        Set<String> words = keywords;

        if (words == null) {
            words = new HashSet<String>();
            Utils.loadFromFile("META-INF/druid/parser/oracle/keywords", words);
            keywords = words;
        }

        return words.contains(name_lower);
    }

    public static List<String> showTables(Connection conn) throws SQLException {
        List<String> tables = new ArrayList<String>();

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select table_name from user_tables");
            while (rs.next()) {
                String tableName = rs.getString(1);
                tables.add(tableName);
            }
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }

        return tables;
    }

    public static List<String> getTableDDL(Connection conn, String... tables) throws SQLException {
        return getTableDDL(conn, Arrays.asList(tables));
    }

    public static List<String> getTableDDL(Connection conn, List<String> tables) throws SQLException {
        List<String> ddlList = new ArrayList<String>();

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            String sql = "select DBMS_METADATA.GET_DDL('TABLE', TABLE_NAME) FROM user_tables";

            if (tables.size() > 0) {
                sql += "IN (";
                for (int i = 0; i < tables.size(); ++i) {
                    if (i != 0) {
                        sql += ", ?";
                    } else {
                        sql += "?";
                    }
                }
                sql += ")";
            }
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < tables.size(); ++i) {
                pstmt.setString(i + 1, tables.get(i));
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String ddl = rs.getString(1);
                ddlList.add(ddl);
            }
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(pstmt);
        }

        return ddlList;
    }

    public static String getCreateTableScript(Connection conn) throws SQLException {
        return getCreateTableScript(conn, true, true);
    }

    public static String getCreateTableScript(Connection conn, boolean sorted, boolean simplify) throws SQLException {
        List<String> ddlList = OracleUtils.getTableDDL(conn);

        StringBuilder buf = new StringBuilder();
        for (String ddl : ddlList) {
            buf.append(ddl);
            buf.append(';');
        }

        String ddlScript = buf.toString();

        if (!(sorted || simplify)) {
            return ddlScript;
        }

        List stmtList = SQLUtils.parseStatements(ddlScript, DbType.oracle);
        if (simplify) {
            for (Object o : stmtList) {
                if (o instanceof SQLCreateTableStatement) {
                    SQLCreateTableStatement createTableStmt = (SQLCreateTableStatement) o;
                    createTableStmt.simplify();
                }
            }
        }

        if (sorted) {
            SQLCreateTableStatement.sort(stmtList);
        }
        return SQLUtils.toSQLString(stmtList, DbType.oracle);
    }
}
