package wang.yeting.sql.dialect.odps.ast;

import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.statement.SQLAlterTableItem;
import wang.yeting.sql.dialect.odps.visitor.OdpsASTVisitor;

public class OdpsAlterTableSetFileFormat extends OdpsObjectImpl
        implements SQLAlterTableItem {
    private SQLExpr value;

    @Override
    public void accept0(OdpsASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, value);
        }
        v.endVisit(this);
    }

    public SQLExpr getValue() {
        return value;
    }

    public void setValue(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.value = x;
    }
}
