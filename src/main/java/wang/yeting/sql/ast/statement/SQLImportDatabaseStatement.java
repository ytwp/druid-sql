package wang.yeting.sql.ast.statement;

import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class SQLImportDatabaseStatement extends SQLStatementImpl {
    private SQLName db;
    private SQLName status;

    public SQLName getDb() {
        return db;
    }

    public void setDb(SQLName db) {
        this.db = db;
    }

    public SQLName getStatus() {
        return status;
    }

    public void setStatus(SQLName status) {
        this.status = status;
    }

    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, db);
            acceptChild(v, status);
        }
        v.endVisit(this);
    }
}
