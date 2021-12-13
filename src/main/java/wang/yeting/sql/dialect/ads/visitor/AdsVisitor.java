package wang.yeting.sql.dialect.ads.visitor;

import wang.yeting.sql.dialect.mysql.ast.MySqlPrimaryKey;
import wang.yeting.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import wang.yeting.sql.visitor.SQLASTVisitor;

public interface AdsVisitor extends SQLASTVisitor {
    boolean visit(MySqlPrimaryKey x);

    void endVisit(MySqlPrimaryKey x);

    boolean visit(MySqlCreateTableStatement x);

    void endVisit(MySqlCreateTableStatement x);
}
