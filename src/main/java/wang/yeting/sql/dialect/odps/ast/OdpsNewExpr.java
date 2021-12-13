package wang.yeting.sql.dialect.odps.ast;

import wang.yeting.sql.FastsqlException;
import wang.yeting.sql.ast.SQLDataType;
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.expr.SQLMethodInvokeExpr;
import wang.yeting.sql.dialect.odps.visitor.OdpsASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OdpsNewExpr extends SQLMethodInvokeExpr implements OdpsObject {
    private boolean array = false;

    private List<SQLExpr> initValues = new ArrayList<>();
    private List<SQLDataType> typeParameters = new ArrayList<>();

    public OdpsNewExpr() {

    }

    @Override
    public OdpsNewExpr clone() {
        OdpsNewExpr x = new OdpsNewExpr();
        cloneTo(x);
        return x;
    }

    @Override
    public void accept0(SQLASTVisitor v) {
        accept0((OdpsASTVisitor) v);
    }

    @Override
    public void accept0(OdpsASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (this.owner != null) {
                this.owner.accept(visitor);
            }

            for (SQLExpr arg : this.arguments) {
                if (arg != null) {
                    arg.accept(visitor);
                }
            }

            if (this.from != null) {
                this.from.accept(visitor);
            }

            if (this.using != null) {
                this.using.accept(visitor);
            }

            if (this._for != null) {
                this._for.accept(visitor);
            }

            visitor.endVisit(this);
        }
        visitor.endVisit(this);
    }

    public void output(Appendable buf) {
        try {
            buf.append("new ");
        } catch (IOException ex) {
            throw new FastsqlException("output error", ex);
        }
        super.output(buf);
    }

    public boolean isArray() {
        return array;
    }

    public void setArray(boolean array) {
        this.array = array;
    }

    public List<SQLExpr> getInitValues() {
        return initValues;
    }

    public List<SQLDataType> getTypeParameters() {
        return typeParameters;
    }
}
