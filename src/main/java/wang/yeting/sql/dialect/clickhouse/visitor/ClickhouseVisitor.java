package wang.yeting.sql.dialect.clickhouse.visitor;

import wang.yeting.sql.dialect.clickhouse.ast.ClickhouseCreateTableStatement;
import wang.yeting.sql.visitor.SQLASTVisitor;

public interface ClickhouseVisitor extends SQLASTVisitor {
    default boolean visit(ClickhouseCreateTableStatement x) {
        return true;
    }

    default void endVisit(ClickhouseCreateTableStatement x) {
    }
}
