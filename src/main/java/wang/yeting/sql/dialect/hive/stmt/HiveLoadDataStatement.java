package wang.yeting.sql.dialect.hive.stmt;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLObject;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.ast.statement.SQLAssignItem;
import wang.yeting.sql.ast.statement.SQLExprTableSource;
import wang.yeting.sql.ast.statement.SQLExternalRecordFormat;
import wang.yeting.sql.dialect.hive.visitor.HiveASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HiveLoadDataStatement extends SQLStatementImpl {
    private final List<SQLExpr> partition = new ArrayList<SQLExpr>(4);
    protected Map<String, SQLObject> serdeProperties = new LinkedHashMap<String, SQLObject>();
    protected SQLExpr using;
    private boolean local;
    private SQLExpr inpath;
    private boolean overwrite;
    private SQLExprTableSource into;
    private SQLExternalRecordFormat format;
    private SQLExpr storedBy;
    private SQLExpr storedAs;
    private SQLExpr rowFormat;

    public HiveLoadDataStatement() {
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
            this.acceptChild(visitor, inpath);
            this.acceptChild(visitor, into);
            this.acceptChild(visitor, partition);
            this.acceptChild(visitor, storedAs);
            this.acceptChild(visitor, storedBy);
            this.acceptChild(visitor, rowFormat);
        }
        visitor.endVisit(this);
    }

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }

    public SQLExpr getInpath() {
        return inpath;
    }

    public void setInpath(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.inpath = x;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public SQLExprTableSource getInto() {
        return into;
    }

    public void setInto(SQLExprTableSource x) {
        if (x != null) {
            x.setParent(this);
        }
        this.into = x;
    }

    public void setInto(SQLExpr x) {
        if (x == null) {
            this.into = null;
            return;
        }

        setInto(new SQLExprTableSource(x));
    }

    public List<SQLExpr> getPartition() {
        return partition;
    }

    public void addPartion(SQLAssignItem item) {
        if (item != null) {
            item.setParent(this);
        }
        this.partition.add(item);
    }

    public SQLExternalRecordFormat getFormat() {
        return format;
    }

    public void setFormat(SQLExternalRecordFormat x) {
        if (x != null) {
            x.setParent(this);
        }
        this.format = x;
    }

    public SQLExpr getStoredBy() {
        return storedBy;
    }

    public void setStoredBy(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.storedBy = x;
    }

    public SQLExpr getStoredAs() {
        return storedAs;
    }

    public void setStoredAs(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.storedAs = x;
    }

    public SQLExpr getRowFormat() {
        return rowFormat;
    }

    public void setRowFormat(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }

        this.rowFormat = x;
    }

    public SQLExpr getUsing() {
        return using;
    }

    public void setUsing(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }

        this.using = x;
    }

    public Map<String, SQLObject> getSerdeProperties() {
        return serdeProperties;
    }
}
