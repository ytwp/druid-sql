package wang.yeting.sql.ast.statement;

import wang.yeting.sql.ast.SQLObjectImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class SQLTableLike extends SQLObjectImpl implements SQLTableElement {
    private SQLExprTableSource table;
    private boolean includeProperties = false;
    private boolean excludeProperties = false;

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, table);
        }
        v.endVisit(this);
    }

    public SQLTableLike clone() {
        SQLTableLike x = new SQLTableLike();
        if (table != null) {
            x.setTable(table.clone());
        }

        return x;
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

    public boolean isIncludeProperties() {
        return includeProperties;
    }

    public void setIncludeProperties(boolean includeProperties) {
        this.includeProperties = includeProperties;
    }

    public boolean isExcludeProperties() {
        return excludeProperties;
    }

    public void setExcludeProperties(boolean excludeProperties) {
        this.excludeProperties = excludeProperties;
    }
}
