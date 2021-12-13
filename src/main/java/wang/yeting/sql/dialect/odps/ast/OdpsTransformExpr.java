package wang.yeting.sql.dialect.odps.ast;

import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLExprImpl;
import wang.yeting.sql.ast.statement.SQLColumnDefinition;
import wang.yeting.sql.ast.statement.SQLExternalRecordFormat;
import wang.yeting.sql.dialect.odps.visitor.OdpsASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class OdpsTransformExpr extends SQLExprImpl implements OdpsObject {
    private final List<SQLExpr> inputColumns = new ArrayList<>();
    private final List<SQLColumnDefinition> outputColumns = new ArrayList<>();
    private final List<SQLExpr> resources = new ArrayList<>();
    private SQLExternalRecordFormat inputRowFormat;
    private SQLExpr using;
    private SQLExternalRecordFormat outputRowFormat;

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        accept0((OdpsASTVisitor) v);
    }

    @Override
    public void accept0(OdpsASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, inputColumns);
            acceptChild(v, outputColumns);
        }
        v.endVisit(this);
    }

    @Override
    public SQLExpr clone() {
        return null;
    }

    public SQLExternalRecordFormat getInputRowFormat() {
        return inputRowFormat;
    }

    public void setInputRowFormat(SQLExternalRecordFormat inputRowFormat) {
        this.inputRowFormat = inputRowFormat;
    }

    public List<SQLExpr> getInputColumns() {
        return inputColumns;
    }

    public List<SQLColumnDefinition> getOutputColumns() {
        return outputColumns;
    }

    public SQLExpr getUsing() {
        return using;
    }

    public void setUsing(SQLExpr using) {
        this.using = using;
    }

    public SQLExternalRecordFormat getOutputRowFormat() {
        return outputRowFormat;
    }

    public void setOutputRowFormat(SQLExternalRecordFormat outputRowFormat) {
        this.outputRowFormat = outputRowFormat;
    }

    public List<SQLExpr> getResources() {
        return resources;
    }
}
