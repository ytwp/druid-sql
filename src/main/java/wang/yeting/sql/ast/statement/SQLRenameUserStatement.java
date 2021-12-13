package wang.yeting.sql.ast.statement;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class SQLRenameUserStatement extends SQLStatementImpl {
    private SQLName name;
    private SQLName to;

    public SQLRenameUserStatement() {
        dbType = DbType.mysql;
    }

    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, name);
            acceptChild(v, to);
        }
        v.endVisit(this);
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        this.name = name;
    }

    public SQLName getTo() {
        return to;
    }

    public void setTo(SQLName to) {
        this.to = to;
    }
}
