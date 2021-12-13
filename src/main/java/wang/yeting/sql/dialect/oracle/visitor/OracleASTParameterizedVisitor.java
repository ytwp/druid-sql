package wang.yeting.sql.dialect.oracle.visitor;

import wang.yeting.sql.DbType;
import wang.yeting.sql.visitor.SQLASTParameterizedVisitor;

import java.util.List;

public class OracleASTParameterizedVisitor extends SQLASTParameterizedVisitor implements OracleASTVisitor {
    public OracleASTParameterizedVisitor() {
        super(DbType.oracle);
    }

    public OracleASTParameterizedVisitor(List<Object> parameters) {
        super(DbType.oracle, parameters);
    }
}
