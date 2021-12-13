package wang.yeting.sql.dialect.postgresql.ast.stmt;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.dialect.postgresql.visitor.PGASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class PGConnectToStatement extends SQLStatementImpl implements PGSQLStatement {
    private SQLName target;

    public PGConnectToStatement() {
        super(DbType.postgresql);
    }

    protected void accept0(SQLASTVisitor visitor) {
        this.accept0((PGASTVisitor) visitor);
    }

    @Override
    public void accept0(PGASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, target);
        }
        v.endVisit(this);
    }

    public SQLName getTarget() {
        return target;
    }

    public void setTarget(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }
        this.target = x;
    }
}
