package wang.yeting.sql.ast.statement;

import wang.yeting.sql.ast.SQLObjectImpl;

public abstract class SQLSelectQueryBase extends SQLObjectImpl implements SQLSelectQuery {
    protected boolean parenthesized;

    @Override
    public boolean isParenthesized() {
        return parenthesized;
    }

    @Override
    public void setParenthesized(boolean parenthesized) {
        this.parenthesized = parenthesized;
    }

    public abstract SQLSelectQueryBase clone();
}
