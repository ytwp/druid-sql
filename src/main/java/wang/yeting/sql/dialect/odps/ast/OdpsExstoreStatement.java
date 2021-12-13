package wang.yeting.sql.dialect.odps.ast;

import wang.yeting.sql.ast.statement.SQLAssignItem;
import wang.yeting.sql.ast.statement.SQLExprTableSource;
import wang.yeting.sql.dialect.odps.visitor.OdpsASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class OdpsExstoreStatement extends OdpsStatementImpl {
    private final List<SQLAssignItem> partitions = new ArrayList<>();
    private SQLExprTableSource table;

    @Override
    protected void accept0(OdpsASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, table);
            acceptChild(v, partitions);
        }
        v.endVisit(this);
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

    public List<SQLAssignItem> getPartitions() {
        return partitions;
    }
}
