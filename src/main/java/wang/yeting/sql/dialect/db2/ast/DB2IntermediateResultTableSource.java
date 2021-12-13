package wang.yeting.sql.dialect.db2.ast;

import wang.yeting.sql.ast.statement.SQLTableSourceImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class DB2IntermediateResultTableSource extends SQLTableSourceImpl {
    @Override
    protected void accept0(SQLASTVisitor v) {

    }

    public static enum Type {
        OldTable,
        NewTable,
        FinalTable
    }
}
