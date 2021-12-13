package wang.yeting.sql.ast.statement;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLExportTableStatement extends SQLStatementImpl {
    private SQLExprTableSource table;
    private List<SQLAssignItem> partition = new ArrayList<SQLAssignItem>();
    private SQLExpr to;
    private SQLExpr forReplication;

    public SQLExportTableStatement() {
        dbType = DbType.hive;
    }

    public SQLExprTableSource getTable() {
        return table;
    }

    public void setTable(SQLExprTableSource x) {
        if (x != null) {
            x.setParent(this);
        }
        this.table = x;
    }

    public List<SQLAssignItem> getPartition() {
        return partition;
    }

    public SQLExpr getTo() {
        return to;
    }

    public void setTo(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.to = x;
    }

    public SQLExpr getForReplication() {
        return forReplication;
    }

    public void setForReplication(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.forReplication = x;
    }

    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, table);
            acceptChild(v, partition);
            acceptChild(v, to);
            acceptChild(v, forReplication);
        }
        v.endVisit(this);
    }
}
