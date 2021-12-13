package wang.yeting.sql.dialect.ads.parser;

import wang.yeting.sql.ast.SQLStatement;
import wang.yeting.sql.ast.statement.*;
import wang.yeting.sql.parser.*;
import wang.yeting.sql.util.FnvHash;

public class AdsStatementParser extends SQLStatementParser {
    public AdsStatementParser(String sql) {
        super(new AdsExprParser(sql));
    }

    public AdsStatementParser(String sql, SQLParserFeature... features) {
        super(new AdsExprParser(sql, features));
    }

    public AdsStatementParser(Lexer lexer) {
        super(new AdsExprParser(lexer));
    }

    public AdsSelectParser createSQLSelectParser() {
        return new AdsSelectParser(this.exprParser, selectListCache);
    }

    public SQLCreateTableParser getSQLCreateTableParser() {
        return new AdsCreateTableParser(this.exprParser);
    }

    public SQLCreateTableStatement parseCreateTable() {
        AdsCreateTableParser parser = new AdsCreateTableParser(this.exprParser);
        return parser.parseCreateTable(true);
    }

    public SQLStatement parseShow() {
        accept(Token.SHOW);

        if (lexer.identifierEquals(FnvHash.Constants.DATABASES)) {
            lexer.nextToken();

            SQLShowDatabasesStatement stmt = parseShowDatabases(false);

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.TABLES)) {
            lexer.nextToken();

            SQLShowTablesStatement stmt = parseShowTables();

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.COLUMNS)) {
            lexer.nextToken();

            SQLShowColumnsStatement stmt = parseShowColumns();

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.TABLEGROUPS)) {
            lexer.nextToken();

            SQLShowTableGroupsStatement stmt = parseShowTableGroups();

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.PROCESSLIST)) {
            lexer.nextToken();

            SQLShowProcessListStatement stmt = new SQLShowProcessListStatement();
            if (lexer.identifierEquals(FnvHash.Constants.MPP)) {
                lexer.nextToken();
                stmt.setMpp(true);
            }

            return stmt;
        }

        if (lexer.token() == Token.CREATE) {
            lexer.nextToken();

            accept(Token.TABLE);

            SQLShowCreateTableStatement stmt = new SQLShowCreateTableStatement();
            stmt.setName(this.exprParser.name());
            return stmt;
        }

        if (lexer.token() == Token.ALL) {
            lexer.nextToken();
            if (lexer.token() == Token.CREATE) {
                lexer.nextToken();

                accept(Token.TABLE);

                SQLShowCreateTableStatement stmt = new SQLShowCreateTableStatement();
                stmt.setAll(true);
                stmt.setName(this.exprParser.name());
                return stmt;
            }

        }

        throw new ParserException("TODO " + lexer.info());
    }
}
