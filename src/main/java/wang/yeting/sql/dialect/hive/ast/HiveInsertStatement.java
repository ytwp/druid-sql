package wang.yeting.sql.dialect.hive.ast;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.SQLCommentHint;
import wang.yeting.sql.ast.SQLStatement;
import wang.yeting.sql.ast.statement.SQLAssignItem;
import wang.yeting.sql.ast.statement.SQLInsertStatement;
import wang.yeting.sql.dialect.hive.visitor.HiveASTVisitor;
import wang.yeting.sql.dialect.odps.visitor.OdpsASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class HiveInsertStatement extends SQLInsertStatement implements SQLStatement {
    private boolean ifNotExists = false;

    public HiveInsertStatement() {
        dbType = DbType.hive;
        partitions = new ArrayList<SQLAssignItem>();
    }

    public HiveInsertStatement clone() {
        HiveInsertStatement x = new HiveInsertStatement();
        super.cloneTo(x);
        return x;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof OdpsASTVisitor) {
            accept0((OdpsASTVisitor) visitor);
        } else if (visitor instanceof HiveASTVisitor) {
            accept0((HiveASTVisitor) visitor);
        } else {
            super.accept0(visitor);
        }
    }

    protected void accept0(HiveASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, tableSource);
            acceptChild(visitor, partitions);
            acceptChild(visitor, valuesList);
            acceptChild(visitor, query);
        }
        visitor.endVisit(this);
    }

    protected void accept0(OdpsASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, tableSource);
            acceptChild(visitor, partitions);
            acceptChild(visitor, valuesList);
            acceptChild(visitor, query);
        }
        visitor.endVisit(this);
    }

    @Override
    public List<SQLCommentHint> getHeadHintsDirect() {
        return null;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }
}
