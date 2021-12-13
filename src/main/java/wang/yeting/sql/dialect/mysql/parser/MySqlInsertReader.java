package wang.yeting.sql.dialect.mysql.parser;

import wang.yeting.sql.ast.statement.SQLInsertStatement;
import wang.yeting.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import wang.yeting.sql.parser.SQLParserFeature;
import wang.yeting.sql.util.JdbcUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;

public class MySqlInsertReader implements Closeable {
    private final Reader in;
    MySqlStatementParser parser;
    private char[] buf = new char[1024];
    private int pos;
    private char ch;
    private MySqlInsertStatement statement;

    public MySqlInsertReader(Reader in) {
        this.in = in;

    }

    public MySqlInsertStatement parseStatement() throws IOException {
        in.read(buf);
        String text = new String(buf);
        parser = new MySqlStatementParser(text, SQLParserFeature.InsertReader);

        statement = (MySqlInsertStatement) parser.parseStatement();
        this.pos = parser.getLexer().pos() - 1;
        this.ch = buf[pos];

        return statement;
    }

    public MySqlInsertStatement getStatement() {
        return statement;
    }

    public SQLInsertStatement.ValuesClause readCaluse() {
        return null;
    }

    public boolean isEOF() {
        return false;
    }

    @Override
    public void close() {
        JdbcUtils.close(in);
    }
}
