package wang.yeting.sql.ast.statement;

import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLObjectImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLPrivilegeItem extends SQLObjectImpl {

    private SQLExpr action;
    private List<SQLName> columns = new ArrayList<SQLName>();

    public SQLExpr getAction() {
        return action;
    }

    public void setAction(SQLExpr action) {
        this.action = action;
    }

    public List<SQLName> getColumns() {
        return columns;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, action);
            acceptChild(v, this.columns);
        }
        v.endVisit(this);
    }
}