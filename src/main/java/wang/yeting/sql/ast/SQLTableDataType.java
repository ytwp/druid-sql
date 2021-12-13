package wang.yeting.sql.ast;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.statement.SQLColumnDefinition;
import wang.yeting.sql.util.FnvHash;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SQLTableDataType extends SQLObjectImpl implements SQLDataType {
    private DbType dbType;
    private List<SQLColumnDefinition> columns = new ArrayList<SQLColumnDefinition>();

    public SQLTableDataType() {

    }

    public SQLTableDataType(DbType dbType) {
        this.dbType = dbType;
    }

    @Override
    public String getName() {
        return "TABLE";
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long nameHashCode64() {
        return FnvHash.Constants.TABLE;
    }

    @Override
    public List<SQLExpr> getArguments() {
        return Collections.emptyList();
    }

    @Override
    public Boolean getWithTimeZone() {
        return null;
    }

    @Override
    public void setWithTimeZone(Boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWithLocalTimeZone() {
        return false;
    }

    @Override
    public void setWithLocalTimeZone(boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DbType getDbType() {
        return dbType;
    }

    @Override
    public void setDbType(DbType dbType) {
        this.dbType = dbType;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, columns);
        }
        visitor.endVisit(this);
    }

    public SQLTableDataType clone() {
        SQLTableDataType x = new SQLTableDataType(dbType);

        for (SQLColumnDefinition item : columns) {
            SQLColumnDefinition item2 = item.clone();
            item2.setParent(x);
            x.columns.add(item2);
        }

        return x;
    }

    public List<SQLColumnDefinition> getColumns() {
        return columns;
    }

    public int jdbcType() {
        return Types.STRUCT;
    }

    @Override
    public boolean isInt() {
        return false;
    }

    @Override
    public boolean isNumberic() {
        return false;
    }

    @Override
    public boolean isString() {
        return false;
    }

    @Override
    public boolean hasKeyLength() {
        return false;
    }

}
