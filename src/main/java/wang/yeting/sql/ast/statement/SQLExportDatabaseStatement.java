package wang.yeting.sql.ast.statement;

import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class SQLExportDatabaseStatement extends SQLStatementImpl {
    private SQLName db;
    private boolean realtime = false;

    public SQLName getDb() {
        return db;
    }

    public void setDb(SQLName db) {
        this.db = db;
    }

    public boolean isRealtime() {
        return realtime;
    }

    public void setRealtime(boolean realtime) {
        this.realtime = realtime;
    }

    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, db);
        }
        v.endVisit(this);
    }
}
