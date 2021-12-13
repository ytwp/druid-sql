package wang.yeting.sql.dialect.hive.stmt;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.ast.statement.SQLAlterStatement;
import wang.yeting.sql.ast.statement.SQLExprTableSource;
import wang.yeting.sql.dialect.hive.visitor.HiveASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class HiveMsckRepairStatement extends SQLStatementImpl implements SQLAlterStatement {
    private SQLExprTableSource table;
    private SQLName database;
    private boolean addPartitions;

    public HiveMsckRepairStatement() {
        super(DbType.hive);
    }

    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof HiveASTVisitor) {
            accept0((HiveASTVisitor) visitor);
        } else {
            super.accept0(visitor);
        }
    }

    protected void accept0(HiveASTVisitor visitor) {
        if (visitor.visit(this)) {
            this.acceptChild(visitor, table);
            this.acceptChild(visitor, database);
        }
        visitor.endVisit(this);
    }

    public boolean isAddPartitions() {
        return addPartitions;
    }

    public void setAddPartitions(boolean addPartitions) {
        this.addPartitions = addPartitions;
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

    public void setTable(SQLExpr x) {
        if (x == null) {
            this.table = null;
            return;
        }

        setTable(new SQLExprTableSource(x));
    }

    public SQLName getDatabase() {
        return database;
    }

    public void setDatabase(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }
        this.database = x;
    }
}
