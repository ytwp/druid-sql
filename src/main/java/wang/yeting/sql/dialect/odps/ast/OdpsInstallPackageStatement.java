package wang.yeting.sql.dialect.odps.ast;

import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.dialect.odps.visitor.OdpsASTVisitor;

public class OdpsInstallPackageStatement extends OdpsStatementImpl {
    private SQLName packageName;

    @Override
    protected void accept0(OdpsASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, packageName);
        }
        v.endVisit(this);
    }

    public SQLName getPackageName() {
        return packageName;
    }

    public void setPackageName(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }
        this.packageName = x;
    }
}
