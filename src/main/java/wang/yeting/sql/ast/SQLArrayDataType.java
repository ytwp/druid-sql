package wang.yeting.sql.ast;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.expr.SQLCharExpr;
import wang.yeting.sql.util.FnvHash;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class SQLArrayDataType extends SQLObjectImpl implements SQLDataType {
    public static final SQLArrayDataType ARRYA_CHAR = new SQLArrayDataType(SQLCharExpr.DATA_TYPE);

    private DbType dbType;
    private SQLDataType componentType;
    private List<SQLExpr> arguments = new ArrayList<SQLExpr>();

    public SQLArrayDataType(SQLDataType componentType) {
        setComponentType(componentType);
    }

    public SQLArrayDataType(SQLDataType componentType, DbType dbType) {
        this.dbType = dbType;
        setComponentType(componentType);
    }

    @Override
    public String getName() {
        return "ARRAY";
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long nameHashCode64() {
        return FnvHash.Constants.ARRAY;
    }

    @Override
    public List<SQLExpr> getArguments() {
        return arguments;
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
            acceptChild(visitor, componentType);
        }
        visitor.endVisit(this);
    }

    public SQLArrayDataType clone() {
        SQLArrayDataType x = new SQLArrayDataType(componentType.clone());
        x.dbType = dbType;

        for (SQLExpr arg : arguments) {
            SQLExpr item = arg.clone();
            item.setParent(x);
            x.arguments.add(item);
        }
        return x;
    }

    public SQLDataType getComponentType() {
        return componentType;
    }

    public void setComponentType(SQLDataType x) {
        if (x != null) {
            x.setParent(this);
        }
        this.componentType = x;
    }

    public int jdbcType() {
        return Types.ARRAY;
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
