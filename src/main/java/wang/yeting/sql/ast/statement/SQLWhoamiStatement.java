package wang.yeting.sql.ast.statement;

import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class SQLWhoamiStatement extends SQLStatementImpl {
    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {

        }
        v.endVisit(this);
    }
}
