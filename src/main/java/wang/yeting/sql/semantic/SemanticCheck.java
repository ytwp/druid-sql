package wang.yeting.sql.semantic;

import wang.yeting.sql.DbType;
import wang.yeting.sql.SQLUtils;
import wang.yeting.sql.ast.SQLStatement;
import wang.yeting.sql.ast.statement.SQLCreateTableStatement;
import wang.yeting.sql.visitor.SQLASTVisitorAdapter;

import java.util.List;

public class SemanticCheck extends SQLASTVisitorAdapter {

    public static boolean check(String sql, DbType dbType) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        SemanticCheck v = new SemanticCheck();
        for (SQLStatement stmt : stmtList) {
            stmt.accept(v);
        }

        return false;
    }

    public boolean visit(SQLCreateTableStatement stmt) {
        stmt.containsDuplicateColumnNames(true);
        return true;
    }
}
