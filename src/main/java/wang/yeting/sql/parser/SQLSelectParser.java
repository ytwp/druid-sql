/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wang.yeting.sql.parser;

import wang.yeting.sql.DbType;
import wang.yeting.sql.dialect.db2.ast.stmt.DB2SelectQueryBlock;
import wang.yeting.sql.dialect.hive.parser.HiveCreateTableParser;
import wang.yeting.sql.dialect.hive.stmt.HiveCreateTableStatement;
import wang.yeting.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import wang.yeting.sql.util.FnvHash;
import wang.yeting.sql.util.StringUtils;

import java.util.List;

public class SQLSelectParser extends SQLParser {
    protected SQLExprParser exprParser;
    protected SQLSelectListCache selectListCache;

    public SQLSelectParser(String sql) {
        super(sql);
    }

    public SQLSelectParser(Lexer lexer) {
        super(lexer);
    }

    public SQLSelectParser(SQLExprParser exprParser) {
        this(exprParser, null);
    }

    public SQLSelectParser(SQLExprParser exprParser, SQLSelectListCache selectListCache) {
        super(exprParser.getLexer(), exprParser.getDbType());
        this.exprParser = exprParser;
        this.selectListCache = selectListCache;
    }

    public wang.yeting.sql.ast.statement.SQLSelect select() {
        wang.yeting.sql.ast.statement.SQLSelect select = new wang.yeting.sql.ast.statement.SQLSelect();

        if (lexer.token == Token.WITH) {
            wang.yeting.sql.ast.statement.SQLWithSubqueryClause with = this.parseWith();
            select.setWithSubQuery(with);
        }

        wang.yeting.sql.ast.statement.SQLSelectQuery query = query(select, true);
        select.setQuery(query);

        wang.yeting.sql.ast.SQLOrderBy orderBy = this.parseOrderBy();

        if (query instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
            wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock = (wang.yeting.sql.ast.statement.SQLSelectQueryBlock) query;

            if (queryBlock.getOrderBy() == null) {
                queryBlock.setOrderBy(orderBy);
                if (lexer.token == Token.LIMIT) {
                    wang.yeting.sql.ast.SQLLimit limit = this.exprParser.parseLimit();
                    queryBlock.setLimit(limit);
                }
            } else {
                select.setOrderBy(orderBy);
                if (lexer.token == Token.LIMIT) {
                    wang.yeting.sql.ast.SQLLimit limit = this.exprParser.parseLimit();
                    select.setLimit(limit);
                }
            }

            if (orderBy != null) {
                parseFetchClause(queryBlock);
            }
        } else {
            select.setOrderBy(orderBy);
        }

        if (lexer.token == Token.LIMIT) {
            wang.yeting.sql.ast.SQLLimit limit = this.exprParser.parseLimit();
            select.setLimit(limit);
        }

        while (lexer.token == Token.HINT) {
            this.exprParser.parseHints(select.getHints());
        }

        return select;
    }

    protected wang.yeting.sql.ast.statement.SQLUnionQuery createSQLUnionQuery() {
        return new wang.yeting.sql.ast.statement.SQLUnionQuery(dbType);
    }

    public wang.yeting.sql.ast.statement.SQLUnionQuery unionRest(wang.yeting.sql.ast.statement.SQLUnionQuery union) {
        if (lexer.token == Token.ORDER) {
            wang.yeting.sql.ast.SQLOrderBy orderBy = this.exprParser.parseOrderBy();
            union.setOrderBy(orderBy);
            return unionRest(union);
        }

        if (lexer.token == Token.LIMIT) {
            wang.yeting.sql.ast.SQLLimit limit = this.exprParser.parseLimit();
            union.setLimit(limit);
        }
        return union;
    }

    public wang.yeting.sql.ast.statement.SQLSelectQuery queryRest(wang.yeting.sql.ast.statement.SQLSelectQuery selectQuery) {
        return queryRest(selectQuery, true);
    }

    public wang.yeting.sql.ast.statement.SQLSelectQuery queryRest(wang.yeting.sql.ast.statement.SQLSelectQuery selectQuery, boolean acceptUnion) {
        if (!acceptUnion) {
            return selectQuery;
        }

        if (lexer.token == Token.UNION) {
            do {
                Lexer.SavePoint uninMark = lexer.mark();
                lexer.nextToken();

                switch (lexer.token) {
                    case GROUP:
                    case ORDER:
                    case WHERE:
                    case RPAREN:
                        lexer.reset(uninMark);
                        return selectQuery;
                    default:
                        break;
                }

                if (lexer.token == Token.SEMI && dbType == DbType.odps) {
                    break;
                }

                wang.yeting.sql.ast.statement.SQLUnionQuery union = createSQLUnionQuery();
                union.setLeft(selectQuery);

                if (lexer.token == Token.ALL) {
                    union.setOperator(wang.yeting.sql.ast.statement.SQLUnionOperator.UNION_ALL);
                    lexer.nextToken();
                } else if (lexer.token == Token.DISTINCT) {
                    union.setOperator(wang.yeting.sql.ast.statement.SQLUnionOperator.DISTINCT);
                    lexer.nextToken();
                }

                boolean paren = lexer.token == Token.LPAREN;
                wang.yeting.sql.ast.statement.SQLSelectQuery right = this.query(paren ? null : union, false);
                union.setRight(right);

                while (lexer.isEnabled(SQLParserFeature.EnableMultiUnion)
                        && lexer.token == Token.UNION
                ) {
                    Lexer.SavePoint mark = lexer.mark();
                    lexer.nextToken();

                    if (lexer.token == Token.UNION && dbType == DbType.odps) {
                        continue; // skip
                    }

                    if (lexer.token == Token.ALL) {
                        if (union.getOperator() == wang.yeting.sql.ast.statement.SQLUnionOperator.UNION_ALL) {
                            lexer.nextToken();
                        } else {
                            lexer.reset(mark);
                            break;
                        }
                    } else if (lexer.token == Token.DISTINCT) {
                        if (union.getOperator() == wang.yeting.sql.ast.statement.SQLUnionOperator.DISTINCT) {
                            lexer.nextToken();
                        } else {
                            lexer.reset(mark);
                            break;
                        }
                    } else if (union.getOperator() == wang.yeting.sql.ast.statement.SQLUnionOperator.UNION) {
                        // skip
                    } else {
                        lexer.reset(mark);
                        break;
                    }

                    paren = lexer.token == Token.LPAREN;
                    wang.yeting.sql.ast.statement.SQLSelectQuery r = this.query(paren ? null : union, false);
                    union.addRelation(r);
                    right = r;
                }

                if (!paren) {
                    if (right instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                        wang.yeting.sql.ast.statement.SQLSelectQueryBlock rightQuery = (wang.yeting.sql.ast.statement.SQLSelectQueryBlock) right;
                        wang.yeting.sql.ast.SQLOrderBy orderBy = rightQuery.getOrderBy();
                        if (orderBy != null) {
                            union.setOrderBy(orderBy);
                            rightQuery.setOrderBy(null);
                        }

                        wang.yeting.sql.ast.SQLLimit limit = rightQuery.getLimit();
                        if (limit != null) {
                            union.setLimit(limit);
                            rightQuery.setLimit(null);
                        }
                    } else if (right instanceof wang.yeting.sql.ast.statement.SQLUnionQuery) {
                        wang.yeting.sql.ast.statement.SQLUnionQuery rightUnion = (wang.yeting.sql.ast.statement.SQLUnionQuery) right;
                        final wang.yeting.sql.ast.SQLOrderBy orderBy = rightUnion.getOrderBy();
                        if (orderBy != null) {
                            union.setOrderBy(orderBy);
                            rightUnion.setOrderBy(null);
                        }

                        wang.yeting.sql.ast.SQLLimit limit = rightUnion.getLimit();
                        if (limit != null) {
                            union.setLimit(limit);
                            rightUnion.setLimit(null);
                        }
                    }
                }

                union = unionRest(union);

                selectQuery = union;

            } while (lexer.token() == Token.UNION);

            selectQuery = queryRest(selectQuery, true);

            return selectQuery;
        }

        if (lexer.token == Token.EXCEPT) {
            lexer.nextToken();

            wang.yeting.sql.ast.statement.SQLUnionQuery union = new wang.yeting.sql.ast.statement.SQLUnionQuery();
            union.setLeft(selectQuery);

            if (lexer.token == Token.ALL) {
                lexer.nextToken();
                union.setOperator(wang.yeting.sql.ast.statement.SQLUnionOperator.EXCEPT_ALL);
            } else if (lexer.token == Token.DISTINCT) {
                lexer.nextToken();
                union.setOperator(wang.yeting.sql.ast.statement.SQLUnionOperator.EXCEPT_DISTINCT);
            } else {
                union.setOperator(wang.yeting.sql.ast.statement.SQLUnionOperator.EXCEPT);
            }

            boolean paren = lexer.token == Token.LPAREN;

            wang.yeting.sql.ast.statement.SQLSelectQuery right = this.query(union, false);
            union.setRight(right);

            if (!paren) {
                if (right instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                    wang.yeting.sql.ast.statement.SQLSelectQueryBlock rightQuery = (wang.yeting.sql.ast.statement.SQLSelectQueryBlock) right;
                    wang.yeting.sql.ast.SQLOrderBy orderBy = rightQuery.getOrderBy();
                    if (orderBy != null) {
                        union.setOrderBy(orderBy);
                        rightQuery.setOrderBy(null);
                    }

                    wang.yeting.sql.ast.SQLLimit limit = rightQuery.getLimit();
                    if (limit != null) {
                        union.setLimit(limit);
                        rightQuery.setLimit(null);
                    }
                } else if (right instanceof wang.yeting.sql.ast.statement.SQLUnionQuery) {
                    wang.yeting.sql.ast.statement.SQLUnionQuery rightUnion = (wang.yeting.sql.ast.statement.SQLUnionQuery) right;
                    final wang.yeting.sql.ast.SQLOrderBy orderBy = rightUnion.getOrderBy();
                    if (orderBy != null) {
                        union.setOrderBy(orderBy);
                        rightUnion.setOrderBy(null);
                    }

                    wang.yeting.sql.ast.SQLLimit limit = rightUnion.getLimit();
                    if (limit != null) {
                        union.setLimit(limit);
                        rightUnion.setLimit(null);
                    }
                }
            }

            return queryRest(union, true);
        }

        if (lexer.token == Token.INTERSECT) {
            lexer.nextToken();

            wang.yeting.sql.ast.statement.SQLUnionQuery union = new wang.yeting.sql.ast.statement.SQLUnionQuery();
            union.setLeft(selectQuery);

            if (lexer.token() == Token.DISTINCT) {
                lexer.nextToken();
                union.setOperator(wang.yeting.sql.ast.statement.SQLUnionOperator.INTERSECT_DISTINCT);
            } else if (lexer.token == Token.ALL) {
                lexer.nextToken();
                union.setOperator(wang.yeting.sql.ast.statement.SQLUnionOperator.INTERSECT_ALL);
            } else {
                union.setOperator(wang.yeting.sql.ast.statement.SQLUnionOperator.INTERSECT);
            }

            boolean paren = lexer.token == Token.LPAREN;
            wang.yeting.sql.ast.statement.SQLSelectQuery right = this.query(union, false);
            union.setRight(right);
            if (!paren) {
                if (right instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                    wang.yeting.sql.ast.statement.SQLSelectQueryBlock rightQuery = (wang.yeting.sql.ast.statement.SQLSelectQueryBlock) right;
                    wang.yeting.sql.ast.SQLOrderBy orderBy = rightQuery.getOrderBy();
                    if (orderBy != null) {
                        union.setOrderBy(orderBy);
                        rightQuery.setOrderBy(null);
                    }

                    wang.yeting.sql.ast.SQLLimit limit = rightQuery.getLimit();
                    if (limit != null) {
                        union.setLimit(limit);
                        rightQuery.setLimit(null);
                    }
                } else if (right instanceof wang.yeting.sql.ast.statement.SQLUnionQuery) {
                    wang.yeting.sql.ast.statement.SQLUnionQuery rightUnion = (wang.yeting.sql.ast.statement.SQLUnionQuery) right;
                    final wang.yeting.sql.ast.SQLOrderBy orderBy = rightUnion.getOrderBy();
                    if (orderBy != null) {
                        union.setOrderBy(orderBy);
                        rightUnion.setOrderBy(null);
                    }

                    wang.yeting.sql.ast.SQLLimit limit = rightUnion.getLimit();
                    if (limit != null) {
                        union.setLimit(limit);
                        rightUnion.setLimit(null);
                    }
                }
            }

            return queryRest(union, true);
        }

        if (acceptUnion && lexer.token == Token.MINUS) {
            lexer.nextToken();

            wang.yeting.sql.ast.statement.SQLUnionQuery union = new wang.yeting.sql.ast.statement.SQLUnionQuery();
            union.setLeft(selectQuery);

            union.setOperator(wang.yeting.sql.ast.statement.SQLUnionOperator.MINUS);
            if (lexer.token == Token.DISTINCT) {
                union.setOperator(wang.yeting.sql.ast.statement.SQLUnionOperator.MINUS_DISTINCT);
                lexer.nextToken();
            } else if (lexer.token == Token.ALL) {
                union.setOperator(wang.yeting.sql.ast.statement.SQLUnionOperator.MINUS_ALL);
                lexer.nextToken();
            }

            wang.yeting.sql.ast.statement.SQLSelectQuery right = this.query(union, false);
            union.setRight(right);

            return queryRest(union, true);
        }

        return selectQuery;
    }

    private void setToLeft(wang.yeting.sql.ast.statement.SQLSelectQuery selectQuery, wang.yeting.sql.ast.statement.SQLUnionQuery parentUnion, wang.yeting.sql.ast.statement.SQLUnionQuery union, wang.yeting.sql.ast.statement.SQLSelectQuery right) {
        wang.yeting.sql.ast.statement.SQLUnionOperator operator = union.getOperator();

        if (union.getLeft() instanceof wang.yeting.sql.ast.statement.SQLUnionQuery) {
            wang.yeting.sql.ast.statement.SQLUnionQuery left = (wang.yeting.sql.ast.statement.SQLUnionQuery) union.getLeft();
            while (left.getLeft() instanceof wang.yeting.sql.ast.statement.SQLUnionQuery) {
                left = (wang.yeting.sql.ast.statement.SQLUnionQuery) left.getLeft();
            }

            left.setLeft(new wang.yeting.sql.ast.statement.SQLUnionQuery(parentUnion.getLeft(), parentUnion.getOperator(), left.getLeft()));

            parentUnion.setLeft(union.getLeft());
            parentUnion.setRight(union.getRight());
        } else {
            parentUnion.setRight(right);
            union.setLeft(parentUnion.getLeft());
            parentUnion.setLeft(union);
            union.setRight(selectQuery);
            union.setOperator(parentUnion.getOperator());
            parentUnion.setOperator(operator);
        }

    }

    public wang.yeting.sql.ast.statement.SQLSelectQuery query() {
        return query(null, true);
    }

    public wang.yeting.sql.ast.statement.SQLSelectQuery query(wang.yeting.sql.ast.SQLObject parent) {
        return query(parent, true);
    }

    public wang.yeting.sql.ast.statement.SQLSelectQuery query(wang.yeting.sql.ast.SQLObject parent, boolean acceptUnion) {
        if (lexer.token == Token.LPAREN) {
            lexer.nextToken();

            wang.yeting.sql.ast.statement.SQLSelectQuery select = query();
            accept(Token.RPAREN);

            return queryRest(select, acceptUnion);
        }

        if (lexer.token() == Token.VALUES) {
            return valuesQuery(acceptUnion);
        }

        wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock = new wang.yeting.sql.ast.statement.SQLSelectQueryBlock(dbType);

        if (lexer.hasComment() && lexer.isKeepComments()) {
            queryBlock.addBeforeComment(lexer.readAndResetComments());
        }

        accept(Token.SELECT);

        if (lexer.token() == Token.HINT) {
            this.exprParser.parseHints(queryBlock.getHints());
        }

        if (lexer.token == Token.COMMENT) {
            lexer.nextToken();
        }

        if (DbType.informix == dbType) {
            if (lexer.identifierEquals(FnvHash.Constants.SKIP)) {
                lexer.nextToken();
                wang.yeting.sql.ast.SQLExpr offset = this.exprParser.primary();
                queryBlock.setOffset(offset);
            }

            if (lexer.identifierEquals(FnvHash.Constants.FIRST)) {
                lexer.nextToken();
                wang.yeting.sql.ast.SQLExpr first = this.exprParser.primary();
                queryBlock.setFirst(first);
            }
        }

        if (lexer.token == Token.DISTINCT) {
            queryBlock.setDistionOption(wang.yeting.sql.ast.SQLSetQuantifier.DISTINCT);
            lexer.nextToken();
        } else if (lexer.token == Token.UNIQUE) {
            queryBlock.setDistionOption(wang.yeting.sql.ast.SQLSetQuantifier.UNIQUE);
            lexer.nextToken();
        } else if (lexer.token == Token.ALL) {
            queryBlock.setDistionOption(wang.yeting.sql.ast.SQLSetQuantifier.ALL);
            lexer.nextToken();
        }

        parseSelectList(queryBlock);

        if (lexer.token() == Token.INTO) {
            lexer.nextToken();

            wang.yeting.sql.ast.SQLExpr expr = expr();
            if (lexer.token() != Token.COMMA) {
                queryBlock.setInto(expr);
            }
        }

        parseFrom(queryBlock);

        parseWhere(queryBlock);

        parseGroupBy(queryBlock);

        if (lexer.identifierEquals(FnvHash.Constants.WINDOW)) {
            parseWindow(queryBlock);
        }

        parseSortBy(queryBlock);

        parseFetchClause(queryBlock);

        if (lexer.token() == Token.FOR) {
            lexer.nextToken();
            accept(Token.UPDATE);

            queryBlock.setForUpdate(true);

            if (lexer.identifierEquals(FnvHash.Constants.NO_WAIT) || lexer.identifierEquals(FnvHash.Constants.NOWAIT)) {
                lexer.nextToken();
                queryBlock.setNoWait(true);
            } else if (lexer.identifierEquals(FnvHash.Constants.WAIT)) {
                lexer.nextToken();
                wang.yeting.sql.ast.SQLExpr waitTime = this.exprParser.primary();
                queryBlock.setWaitTime(waitTime);
            }
        }

        return queryRest(queryBlock, acceptUnion);
    }

    protected wang.yeting.sql.ast.statement.SQLSelectQuery valuesQuery(boolean acceptUnion) {
        lexer.nextToken();
        wang.yeting.sql.ast.statement.SQLValuesQuery valuesQuery = new wang.yeting.sql.ast.statement.SQLValuesQuery();

        for (; ; ) {
            if (lexer.token == Token.LPAREN) {
                lexer.nextToken();
                wang.yeting.sql.ast.expr.SQLListExpr listExpr = new wang.yeting.sql.ast.expr.SQLListExpr();
                this.exprParser.exprList(listExpr.getItems(), listExpr);
                accept(Token.RPAREN);
                valuesQuery.addValue(listExpr);
            } else {
                this.exprParser.exprList(valuesQuery.getValues(), valuesQuery);
            }

            if (lexer.token == Token.COMMA) {
                lexer.nextToken();
                continue;
            } else {
                break;
            }
        }

        return queryRest(valuesQuery, acceptUnion);
    }

    protected void withSubquery(wang.yeting.sql.ast.statement.SQLSelect select) {
        if (lexer.token == Token.WITH) {
            lexer.nextToken();

            wang.yeting.sql.ast.statement.SQLWithSubqueryClause withQueryClause = new wang.yeting.sql.ast.statement.SQLWithSubqueryClause();

            if (lexer.token == Token.RECURSIVE || lexer.identifierEquals(FnvHash.Constants.RECURSIVE)) {
                lexer.nextToken();
                withQueryClause.setRecursive(true);
            }

            for (; ; ) {
                wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry entry = new wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry();
                entry.setParent(withQueryClause);

                String alias = this.lexer.stringVal();
                lexer.nextToken();
                entry.setAlias(alias);

                if (lexer.token == Token.LPAREN) {
                    lexer.nextToken();
                    exprParser.names(entry.getColumns());
                    accept(Token.RPAREN);
                }

                accept(Token.AS);
                accept(Token.LPAREN);
                entry.setSubQuery(select());
                accept(Token.RPAREN);

                withQueryClause.addEntry(entry);

                if (lexer.token == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }

                break;
            }

            select.setWithSubQuery(withQueryClause);
        }
    }

    public wang.yeting.sql.ast.statement.SQLWithSubqueryClause parseWith() {
        wang.yeting.sql.ast.statement.SQLWithSubqueryClause withQueryClause = new wang.yeting.sql.ast.statement.SQLWithSubqueryClause();
        if (lexer.hasComment() && lexer.isKeepComments()) {
            withQueryClause.addBeforeComment(lexer.readAndResetComments());
        }

        accept(Token.WITH);

        if (lexer.token == Token.RECURSIVE || lexer.identifierEquals(FnvHash.Constants.RECURSIVE)) {
            lexer.nextToken();
            withQueryClause.setRecursive(true);
        }

        for (; ; ) {
            wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry entry = new wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry();
            entry.setParent(withQueryClause);

            String alias = this.lexer.stringVal();
            lexer.nextToken();
            entry.setAlias(alias);

            if (lexer.token == Token.LPAREN) {
                lexer.nextToken();
                exprParser.names(entry.getColumns());
                accept(Token.RPAREN);
            }

            accept(Token.AS);
            accept(Token.LPAREN);

            switch (lexer.token) {
                case SELECT:
                case LPAREN:
                case WITH:
                case FROM:
                    entry.setSubQuery(select());
                    break;
                default:
                    break;
            }

            accept(Token.RPAREN);

            withQueryClause.addEntry(entry);

            if (lexer.token == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }

        return withQueryClause;
    }

    public void parseWhere(wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock) {
        if (lexer.token != Token.WHERE) {
            return;
        }

        lexer.nextTokenIdent();

        List<String> beforeComments = null;
        if (lexer.hasComment() && lexer.isKeepComments()) {
            beforeComments = lexer.readAndResetComments();
        }

        wang.yeting.sql.ast.SQLExpr where;

        if (lexer.token == Token.IDENTIFIER) {
            String ident = lexer.stringVal();
            long hash_lower = lexer.hash_lower();
            lexer.nextTokenEq();

            wang.yeting.sql.ast.SQLExpr identExpr;
            if (lexer.token == Token.LITERAL_CHARS) {
                String literal = lexer.stringVal;
                if (hash_lower == FnvHash.Constants.TIMESTAMP) {
                    identExpr = new wang.yeting.sql.ast.expr.SQLTimestampExpr(literal);
                    lexer.nextToken();
                } else if (hash_lower == FnvHash.Constants.DATE) {
                    identExpr = new wang.yeting.sql.ast.expr.SQLDateExpr(literal);
                    lexer.nextToken();
                } else if (hash_lower == FnvHash.Constants.REAL) {
                    identExpr = new wang.yeting.sql.ast.expr.SQLRealExpr(Float.parseFloat(literal));
                    lexer.nextToken();
                } else {
                    identExpr = new wang.yeting.sql.ast.expr.SQLIdentifierExpr(ident, hash_lower);
                }
            } else {
                identExpr = new wang.yeting.sql.ast.expr.SQLIdentifierExpr(ident, hash_lower);
            }

            if (lexer.token == Token.DOT) {
                identExpr = this.exprParser.primaryRest(identExpr);
            }

            if (lexer.token == Token.EQ) {
                wang.yeting.sql.ast.SQLExpr rightExp;

                lexer.nextToken();

                try {
                    rightExp = this.exprParser.bitOr();
                } catch (EOFParserException e) {
                    throw new ParserException("EOF, " + ident + "=", e);
                }

                where = new wang.yeting.sql.ast.expr.SQLBinaryOpExpr(identExpr, wang.yeting.sql.ast.expr.SQLBinaryOperator.Equality, rightExp, dbType);
                switch (lexer.token) {
                    case BETWEEN:
                    case IS:
                    case EQ:
                    case IN:
                    case CONTAINS:
                    case BANG_TILDE_STAR:
                    case TILDE_EQ:
                    case LT:
                    case LTEQ:
                    case LTEQGT:
                    case GT:
                    case GTEQ:
                    case LTGT:
                    case BANGEQ:
                    case LIKE:
                    case NOT:
                        where = this.exprParser.relationalRest(where);
                        break;
                    default:
                        break;
                }

                where = this.exprParser.andRest(where);
                where = this.exprParser.xorRest(where);
                where = this.exprParser.orRest(where);
            } else {
                identExpr = this.exprParser.primaryRest(identExpr);
                where = this.exprParser.exprRest(identExpr);
            }
        } else {
            while (lexer.token == Token.HINT) {
                lexer.nextToken();
            }

            where = this.exprParser.expr();

            while (lexer.token == Token.HINT) {
                lexer.nextToken();
            }
        }
//            where = this.exprParser.expr();

        if (beforeComments != null) {
            where.addBeforeComment(beforeComments);
        }

        if (lexer.hasComment() && lexer.isKeepComments() //
                && lexer.token != Token.INSERT // odps multi-insert
        ) {
            where.addAfterComment(lexer.readAndResetComments());
        }

        queryBlock.setWhere(where);

    }

    protected void parseSortBy(wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock) {
        if (lexer.token() == Token.ORDER) {
            wang.yeting.sql.ast.SQLOrderBy orderBy = parseOrderBy();
            queryBlock.setOrderBy(orderBy);
        }

        if (lexer.identifierEquals(FnvHash.Constants.DISTRIBUTE)) {
            lexer.nextToken();
            accept(Token.BY);

            for (; ; ) {
                wang.yeting.sql.ast.statement.SQLSelectOrderByItem distributeByItem = this.exprParser.parseSelectOrderByItem();
                queryBlock.addDistributeBy(distributeByItem);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                } else {
                    break;
                }
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.SORT)) {
            lexer.nextToken();
            accept(Token.BY);

            for (; ; ) {
                wang.yeting.sql.ast.statement.SQLSelectOrderByItem sortByItem = this.exprParser.parseSelectOrderByItem();
                queryBlock.addSortBy(sortByItem);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                } else {
                    break;
                }
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.CLUSTER)) {
            lexer.nextToken();
            accept(Token.BY);

            for (; ; ) {
                wang.yeting.sql.ast.statement.SQLSelectOrderByItem clusterByItem = this.exprParser.parseSelectOrderByItem();
                queryBlock.addClusterBy(clusterByItem);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                } else {
                    break;
                }
            }
        }
    }

    protected void parseWindow(wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock) {
        if (!(lexer.identifierEquals(FnvHash.Constants.WINDOW) || lexer.token == Token.WINDOW)) {
            return;
        }

        lexer.nextToken();

        for (; ; ) {
            wang.yeting.sql.ast.SQLName name = this.exprParser.name();
            accept(Token.AS);
            wang.yeting.sql.ast.SQLOver over = new wang.yeting.sql.ast.SQLOver();
            this.exprParser.over(over);
            queryBlock.addWindow(new wang.yeting.sql.ast.SQLWindow(name, over));

            if (lexer.token == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }
    }

    public void parseGroupBy(wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock) {
        if (lexer.token == Token.GROUP) {
            lexer.nextTokenBy();
            accept(Token.BY);

            wang.yeting.sql.ast.statement.SQLSelectGroupByClause groupBy = new wang.yeting.sql.ast.statement.SQLSelectGroupByClause();

            if (lexer.token == Token.HINT) {
                groupBy.setHint(this.exprParser.parseHint());
            }

            if (lexer.token == Token.ALL) {
                Lexer.SavePoint mark = lexer.mark();
                lexer.nextToken();
                if (!lexer.identifierEquals(FnvHash.Constants.GROUPING)) {
                    if (dbType == DbType.odps) {
                        lexer.reset(mark);
                    } else {
                        throw new ParserException("group by all syntax error. " + lexer.info());
                    }
                }
            } else if (lexer.token == Token.DISTINCT) {
                lexer.nextToken();
                groupBy.setDistinct(true);
            }

            if (lexer.identifierEquals(FnvHash.Constants.ROLLUP)) {
                lexer.nextToken();
                accept(Token.LPAREN);
                groupBy.setWithRollUp(true);
            }
            if (lexer.identifierEquals(FnvHash.Constants.CUBE)) {
                lexer.nextToken();
                accept(Token.LPAREN);
                groupBy.setWithCube(true);
            }

            for (; ; ) {
                wang.yeting.sql.ast.SQLExpr item = parseGroupByItem();

                item.setParent(groupBy);
                groupBy.addItem(item);

                if (lexer.token == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                } else if (lexer.identifierEquals(FnvHash.Constants.GROUPING)) {
                    continue;
                } else {
                    break;
                }
            }
            if (groupBy.isWithRollUp() || groupBy.isWithCube()) {
                accept(Token.RPAREN);
                groupBy.setParen(true);

                if (lexer.token == Token.COMMA && dbType == DbType.odps) {
                    lexer.nextToken();
                    wang.yeting.sql.ast.expr.SQLMethodInvokeExpr func = new wang.yeting.sql.ast.expr.SQLMethodInvokeExpr(groupBy.isWithCube() ? "CUBE" : "ROLLUP");
                    func.getArguments().addAll(groupBy.getItems());
                    groupBy.getItems().clear();
                    groupBy.setWithCube(false);
                    groupBy.setWithRollUp(false);
                    for (wang.yeting.sql.ast.SQLExpr arg : func.getArguments()) {
                        arg.setParent(func);
                    }
                    groupBy.addItem(func);
                    this.exprParser.exprList(groupBy.getItems(), groupBy);
                }
            }

            if (lexer.token == (Token.HAVING)) {
                lexer.nextToken();

                wang.yeting.sql.ast.SQLExpr having = this.exprParser.expr();
                groupBy.setHaving(having);
            }

            if (lexer.token == Token.WITH) {
                Lexer.SavePoint mark = lexer.mark();
                lexer.nextToken();

                if (lexer.identifierEquals(FnvHash.Constants.CUBE)) {
                    lexer.nextToken();
                    groupBy.setWithCube(true);
                } else if (lexer.identifierEquals(FnvHash.Constants.ROLLUP)) {
                    lexer.nextToken();
                    groupBy.setWithRollUp(true);
                } else if (lexer.identifierEquals(FnvHash.Constants.RS)
                        && DbType.db2 == dbType) {
                    lexer.nextToken();
                    ((DB2SelectQueryBlock) queryBlock).setIsolation(DB2SelectQueryBlock.Isolation.RS);
                } else if (lexer.identifierEquals(FnvHash.Constants.RR)
                        && DbType.db2 == dbType) {
                    lexer.nextToken();
                    ((DB2SelectQueryBlock) queryBlock).setIsolation(DB2SelectQueryBlock.Isolation.RR);
                } else if (lexer.identifierEquals(FnvHash.Constants.CS)
                        && DbType.db2 == dbType) {
                    lexer.nextToken();
                    ((DB2SelectQueryBlock) queryBlock).setIsolation(DB2SelectQueryBlock.Isolation.CS);
                } else if (lexer.identifierEquals(FnvHash.Constants.UR)
                        && DbType.db2 == dbType) {
                    lexer.nextToken();
                    ((DB2SelectQueryBlock) queryBlock).setIsolation(DB2SelectQueryBlock.Isolation.UR);
                } else {
                    lexer.reset(mark);
                }
            }

            if (groupBy.getHaving() == null && lexer.token == Token.HAVING) {
                lexer.nextToken();

                wang.yeting.sql.ast.SQLExpr having = this.exprParser.expr();
                groupBy.setHaving(having);
            }

            queryBlock.setGroupBy(groupBy);
        } else if (lexer.token == (Token.HAVING)) {
            lexer.nextToken();

            wang.yeting.sql.ast.statement.SQLSelectGroupByClause groupBy = new wang.yeting.sql.ast.statement.SQLSelectGroupByClause();
            groupBy.setHaving(this.exprParser.expr());

            if (lexer.token == (Token.GROUP)) {
                lexer.nextToken();
                accept(Token.BY);

                for (; ; ) {
                    wang.yeting.sql.ast.SQLExpr item = parseGroupByItem();

                    item.setParent(groupBy);
                    groupBy.addItem(item);

                    if (!(lexer.token == (Token.COMMA))) {
                        break;
                    }

                    lexer.nextToken();
                }
            }

            if (lexer.token == Token.WITH) {
                lexer.nextToken();
                acceptIdentifier("ROLLUP");

                groupBy.setWithRollUp(true);
            }

            if (DbType.mysql == dbType
                    && lexer.token == Token.DESC) {
                lexer.nextToken(); // skip
            }

            queryBlock.setGroupBy(groupBy);
        }
    }

    protected wang.yeting.sql.ast.SQLExpr parseGroupByItem() {
        if (lexer.token == Token.LPAREN) {
            Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();

            if (lexer.token == Token.RPAREN) {
                lexer.nextToken();
                return new wang.yeting.sql.ast.expr.SQLListExpr();
            }

            lexer.reset(mark);
        }
        wang.yeting.sql.ast.SQLExpr item;
        if (lexer.identifierEquals(FnvHash.Constants.ROLLUP)) {
            wang.yeting.sql.ast.expr.SQLMethodInvokeExpr rollup = new wang.yeting.sql.ast.expr.SQLMethodInvokeExpr(lexer.stringVal());
            lexer.nextToken();
            if (lexer.token == Token.LPAREN) {
                lexer.nextToken();
                for (; ; ) {
                    if (lexer.token == Token.RPAREN) {
                        break;
                    }

                    wang.yeting.sql.ast.SQLExpr expr;
                    if (lexer.token == Token.LPAREN) {
                        accept(Token.LPAREN);
                        wang.yeting.sql.ast.expr.SQLListExpr list = new wang.yeting.sql.ast.expr.SQLListExpr();
                        if (lexer.token == Token.COMMA) {
                            lexer.nextToken();
                        }
                        this.exprParser.exprList(list.getItems(), list);
                        accept(Token.RPAREN);
                        expr = list;
                    } else {
                        expr = this.exprParser.expr();
                    }
                    rollup.addArgument(expr);

                    if (lexer.token == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
            }
            item = rollup;
        } else {
            item = this.exprParser.expr();
        }

        if (DbType.mysql == dbType) {
            if (lexer.token == Token.DESC) {
                lexer.nextToken(); // skip
                item = new MySqlOrderingExpr(item, wang.yeting.sql.ast.SQLOrderingSpecification.DESC);
            } else if (lexer.token == Token.ASC) {
                lexer.nextToken(); // skip
                item = new MySqlOrderingExpr(item, wang.yeting.sql.ast.SQLOrderingSpecification.ASC);
            }
        }

        if (lexer.token == Token.HINT) {
            wang.yeting.sql.ast.SQLCommentHint hint = this.exprParser.parseHint();// skip
            if (item instanceof wang.yeting.sql.ast.SQLObjectImpl) {
                ((wang.yeting.sql.ast.SQLExprImpl) item).setHint(hint);
            }
        }

        return item;
    }

    protected void parseSelectList(wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock) {
        final List<wang.yeting.sql.ast.statement.SQLSelectItem> selectList = queryBlock.getSelectList();
        for (; ; ) {
            final wang.yeting.sql.ast.statement.SQLSelectItem selectItem = this.exprParser.parseSelectItem();
            selectList.add(selectItem);
            selectItem.setParent(queryBlock);

            if (lexer.token != Token.COMMA) {
                break;
            }

            lexer.nextToken();
        }
    }

    public void parseFrom(wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock) {
        if (lexer.token != Token.FROM) {
            return;
        }

        lexer.nextToken();

        queryBlock.setFrom(
                parseTableSource());
    }

    public wang.yeting.sql.ast.statement.SQLTableSource parseTableSource() {
        if (lexer.token == Token.LPAREN) {
            lexer.nextToken();
            wang.yeting.sql.ast.statement.SQLTableSource tableSource;
            if (lexer.token == Token.SELECT || lexer.token == Token.WITH
                    || lexer.token == Token.SEL) {
                wang.yeting.sql.ast.statement.SQLSelect select = select();
                accept(Token.RPAREN);
                wang.yeting.sql.ast.statement.SQLSelectQuery selectQuery = select.getQuery();
                selectQuery.setParenthesized(true);

                boolean acceptUnion = !(selectQuery instanceof wang.yeting.sql.ast.statement.SQLUnionQuery);
                wang.yeting.sql.ast.statement.SQLSelectQuery query = queryRest(selectQuery, acceptUnion);
                if (query instanceof wang.yeting.sql.ast.statement.SQLUnionQuery) {
                    tableSource = new wang.yeting.sql.ast.statement.SQLUnionQueryTableSource((wang.yeting.sql.ast.statement.SQLUnionQuery) query);
                } else {
                    tableSource = new wang.yeting.sql.ast.statement.SQLSubqueryTableSource(select);
                }
            } else if (lexer.token == Token.LPAREN) {
                tableSource = parseTableSource();

                while ((lexer.token == Token.UNION
                        || lexer.token == Token.EXCEPT
                        || lexer.token == Token.INTERSECT
                        || lexer.token == Token.MINUS)
                        && tableSource instanceof wang.yeting.sql.ast.statement.SQLUnionQueryTableSource) {
                    wang.yeting.sql.ast.statement.SQLUnionQueryTableSource unionQueryTableSource = (wang.yeting.sql.ast.statement.SQLUnionQueryTableSource) tableSource;
                    wang.yeting.sql.ast.statement.SQLUnionQuery union = unionQueryTableSource.getUnion();
                    unionQueryTableSource.setUnion(
                            (wang.yeting.sql.ast.statement.SQLUnionQuery) queryRest(union)
                    );
                }
                accept(Token.RPAREN);
            } else {
                tableSource = parseTableSource();
                accept(Token.RPAREN);
            }

            if (lexer.token == Token.AS) {
                lexer.nextToken();
                String alias = this.tableAlias(true);
                tableSource.setAlias(alias);

                if (tableSource instanceof wang.yeting.sql.ast.statement.SQLValuesTableSource
                        && ((wang.yeting.sql.ast.statement.SQLValuesTableSource) tableSource).getColumns().size() == 0) {
                    wang.yeting.sql.ast.statement.SQLValuesTableSource values = (wang.yeting.sql.ast.statement.SQLValuesTableSource) tableSource;
                    accept(Token.LPAREN);
                    this.exprParser.names(values.getColumns(), values);
                    accept(Token.RPAREN);
                } else if (tableSource instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource) {
                    wang.yeting.sql.ast.statement.SQLSubqueryTableSource values = (wang.yeting.sql.ast.statement.SQLSubqueryTableSource) tableSource;
                    if (lexer.token == Token.LPAREN) {
                        lexer.nextToken();
                        this.exprParser.names(values.getColumns(), values);
                        accept(Token.RPAREN);
                    }
                }
            }

            return parseTableSourceRest(tableSource);
        }

        if (lexer.token() == Token.VALUES) {
            lexer.nextToken();
            wang.yeting.sql.ast.statement.SQLValuesTableSource tableSource = new wang.yeting.sql.ast.statement.SQLValuesTableSource();

            for (; ; ) {
                accept(Token.LPAREN);
                wang.yeting.sql.ast.expr.SQLListExpr listExpr = new wang.yeting.sql.ast.expr.SQLListExpr();
                this.exprParser.exprList(listExpr.getItems(), listExpr);
                accept(Token.RPAREN);

                listExpr.setParent(tableSource);

                tableSource.getValues().add(listExpr);

                if (lexer.token == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }

            if (lexer.token == Token.RPAREN) {
                return tableSource;
            }

            String alias = this.tableAlias();
            if (alias != null) {
                tableSource.setAlias(alias);
            }

            accept(Token.LPAREN);
            this.exprParser.names(tableSource.getColumns(), tableSource);
            accept(Token.RPAREN);

            return parseTableSourceRest(tableSource);
        }

        if (lexer.token == Token.SELECT) {
            throw new ParserException("TODO " + lexer.info());
        }

        wang.yeting.sql.ast.statement.SQLExprTableSource tableReference = new wang.yeting.sql.ast.statement.SQLExprTableSource();

        parseTableSourceQueryTableExpr(tableReference);

        wang.yeting.sql.ast.statement.SQLTableSource tableSrc = parseTableSourceRest(tableReference);

        if (lexer.hasComment() && lexer.isKeepComments()) {
            tableSrc.addAfterComment(lexer.readAndResetComments());
        }

        return tableSrc;
    }

    protected void parseTableSourceQueryTableExpr(wang.yeting.sql.ast.statement.SQLExprTableSource tableReference) {
        if (lexer.token == Token.LITERAL_ALIAS || lexer.identifierEquals(FnvHash.Constants.IDENTIFIED)
                || lexer.token == Token.LITERAL_CHARS) {
            tableReference.setExpr(this.exprParser.name());
            return;
        }

        if (lexer.token == Token.HINT) {
            wang.yeting.sql.ast.SQLCommentHint hint = this.exprParser.parseHint();
            tableReference.setHint(hint);
        }

        wang.yeting.sql.ast.SQLExpr expr;
        switch (lexer.token) {
            case ALL:
            case SET:
                expr = this.exprParser.name();
                break;
            default:
                expr = expr();
                break;
        }

        if (expr instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr) {
            throw new ParserException("Invalid from clause : " + expr.toString().replace("\n", " "));
        }

        tableReference.setExpr(expr);
    }

    protected wang.yeting.sql.ast.statement.SQLTableSource primaryTableSourceRest(wang.yeting.sql.ast.statement.SQLTableSource tableSource) {
        return tableSource;
    }

    public void parseTableSourceSample(wang.yeting.sql.ast.statement.SQLTableSource tableSource) {

    }

    public void parseTableSourceSampleHive(wang.yeting.sql.ast.statement.SQLTableSource tableSource) {
        if (lexer.identifierEquals(FnvHash.Constants.TABLESAMPLE) && tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();
            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();

                wang.yeting.sql.ast.statement.SQLTableSampling sampling = new wang.yeting.sql.ast.statement.SQLTableSampling();

                if (lexer.identifierEquals(FnvHash.Constants.BUCKET)) {
                    lexer.nextToken();
                    wang.yeting.sql.ast.SQLExpr bucket = this.exprParser.primary();
                    sampling.setBucket(bucket);

                    if (lexer.token() == Token.OUT) {
                        lexer.nextToken();
                        accept(Token.OF);
                        wang.yeting.sql.ast.SQLExpr outOf = this.exprParser.primary();
                        sampling.setOutOf(outOf);
                    }

                    if (lexer.token() == Token.ON) {
                        lexer.nextToken();
                        wang.yeting.sql.ast.SQLExpr on = this.exprParser.expr();
                        sampling.setOn(on);
                    }
                }

                if (lexer.token() == Token.LITERAL_INT || lexer.token() == Token.LITERAL_FLOAT) {
                    wang.yeting.sql.ast.SQLExpr val = this.exprParser.primary();

                    if (lexer.identifierEquals(FnvHash.Constants.ROWS)) {
                        lexer.nextToken();
                        sampling.setRows(val);
                    } else {
                        acceptIdentifier("PERCENT");
                        sampling.setPercent(val);
                    }
                }

                if (lexer.token() == Token.IDENTIFIER) {
                    String strVal = lexer.stringVal();
                    char first = strVal.charAt(0);
                    char last = strVal.charAt(strVal.length() - 1);
                    if (last >= 'a' && last <= 'z') {
                        last -= 32; // to upper
                    }

                    boolean match = false;
                    if ((first == '.' || (first >= '0' && first <= '9'))) {
                        switch (last) {
                            case 'B':
                            case 'K':
                            case 'M':
                            case 'G':
                            case 'T':
                            case 'P':
                                match = true;
                                break;
                            default:
                                break;
                        }
                    }
                    wang.yeting.sql.ast.expr.SQLSizeExpr size = new wang.yeting.sql.ast.expr.SQLSizeExpr(strVal.substring(0, strVal.length() - 2), last);
                    sampling.setByteLength(size);
                    lexer.nextToken();
                }

                final wang.yeting.sql.ast.statement.SQLExprTableSource table = (wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource;
                table.setSampling(sampling);

                accept(Token.RPAREN);
            } else {
                lexer.reset(mark);
            }
        }
    }

    public wang.yeting.sql.ast.statement.SQLTableSource parseTableSourceRest(wang.yeting.sql.ast.statement.SQLTableSource tableSource) {
        parseTableSourceSample(tableSource);

        if (lexer.hasComment()
                && lexer.isKeepComments()
                && !(tableSource instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource)) {
            tableSource.addAfterComment(lexer.readAndResetComments());
        }

        if (tableSource.getAlias() == null || tableSource.getAlias().length() == 0) {
            Token token = lexer.token;
            long hash;

            switch (token) {
                case LEFT:
                case RIGHT:
                case FULL: {
                    Lexer.SavePoint mark = lexer.mark();
                    String strVal = lexer.stringVal();
                    lexer.nextToken();
                    if (lexer.token == Token.OUTER
                            || lexer.token == Token.JOIN
                            || lexer.identifierEquals(FnvHash.Constants.ANTI)
                            || lexer.identifierEquals(FnvHash.Constants.SEMI)) {
                        lexer.reset(mark);
                    } else {
                        tableSource.setAlias(strVal);
                    }
                }
                break;
                case OUTER:
                    break;
                default:
                    if (!(token == Token.IDENTIFIER
                            && ((hash = lexer.hash_lower()) == FnvHash.Constants.STRAIGHT_JOIN
                            || hash == FnvHash.Constants.CROSS))) {
                        boolean must = false;
                        if (lexer.token == Token.AS) {
                            lexer.nextToken();
                            must = true;
                        }
                        String alias = tableAlias(must);
                        if (alias != null) {
                            if (isEnabled(SQLParserFeature.IgnoreNameQuotes) && alias.length() > 1) {
                                alias = StringUtils.removeNameQuotes(alias);
                            }
                            tableSource.setAlias(alias);

                            if ((tableSource instanceof wang.yeting.sql.ast.statement.SQLValuesTableSource)
                                    && ((wang.yeting.sql.ast.statement.SQLValuesTableSource) tableSource).getColumns().size() == 0) {
                                wang.yeting.sql.ast.statement.SQLValuesTableSource values = (wang.yeting.sql.ast.statement.SQLValuesTableSource) tableSource;
                                accept(Token.LPAREN);
                                this.exprParser.names(values.getColumns(), values);
                                accept(Token.RPAREN);
                            } else if (tableSource instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource) {
                                wang.yeting.sql.ast.statement.SQLSubqueryTableSource subQuery = (wang.yeting.sql.ast.statement.SQLSubqueryTableSource) tableSource;
                                if (lexer.token == Token.LPAREN) {
                                    lexer.nextToken();
                                    this.exprParser.names(subQuery.getColumns(), subQuery);
                                    accept(Token.RPAREN);
                                }
                            } else if (tableSource instanceof wang.yeting.sql.ast.statement.SQLUnionQueryTableSource) {
                                wang.yeting.sql.ast.statement.SQLUnionQueryTableSource union = (wang.yeting.sql.ast.statement.SQLUnionQueryTableSource) tableSource;
                                if (lexer.token == Token.LPAREN) {
                                    lexer.nextToken();
                                    this.exprParser.names(union.getColumns(), union);
                                    accept(Token.RPAREN);
                                }
                            } else if (lexer.token == Token.LPAREN
                                    && tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource
                                    && (((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getExpr() instanceof wang.yeting.sql.ast.expr.SQLVariantRefExpr
                                    || ((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getExpr() instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr
                            )
                            ) {
                                lexer.nextToken();
                                wang.yeting.sql.ast.statement.SQLExprTableSource exprTableSource = (wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource;
                                this.exprParser.names(exprTableSource.getColumns(), exprTableSource);
                                accept(Token.RPAREN);
                            }

                            if (lexer.token == Token.WHERE) {
                                return tableSource;
                            }

                            return parseTableSourceRest(tableSource);
                        }
                    }
                    break;
            }

        }

        wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType joinType = null;

        boolean natural = lexer.identifierEquals(FnvHash.Constants.NATURAL);
        if (natural) {
            lexer.nextToken();
        }

        boolean asof = false;
        if (lexer.identifierEquals(FnvHash.Constants.ASOF) && dbType == DbType.clickhouse) {
            lexer.nextToken();
            asof = true;
        }

        if (lexer.token == Token.OUTER) {
            Lexer.SavePoint mark = lexer.mark();
            String str = lexer.stringVal();
            lexer.nextToken();
            if (tableSource.getAlias() == null &&
                    !lexer.identifierEquals(FnvHash.Constants.APPLY)) {
                tableSource.setAlias(str);
            } else {
                lexer.reset(mark);
            }
        }

        boolean global = false;
        if (dbType == DbType.clickhouse) {
            if (lexer.token == Token.GLOBAL) {
                lexer.nextToken();
                global = true;
            }
        }

        switch (lexer.token) {
            case LEFT:
                lexer.nextToken();

                if (lexer.identifierEquals(FnvHash.Constants.SEMI)) {
                    lexer.nextToken();
                    joinType = wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.LEFT_SEMI_JOIN;
                } else if (lexer.identifierEquals(FnvHash.Constants.ANTI)) {
                    lexer.nextToken();
                    joinType = wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.LEFT_ANTI_JOIN;
                } else if (lexer.token == Token.OUTER) {
                    lexer.nextToken();
                    joinType = wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN;
                } else {
                    joinType = wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN;
                }

                if (dbType == DbType.odps && lexer.token == Token.IDENTIFIER && lexer.stringVal().startsWith("join@")) {
                    lexer.stringVal = lexer.stringVal().substring(5);
                    break;
                }

                accept(Token.JOIN);
                break;
            case RIGHT:
                lexer.nextToken();
                if (lexer.token == Token.OUTER) {
                    lexer.nextToken();
                }
                accept(Token.JOIN);
                joinType = wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN;
                break;
            case FULL:
                lexer.nextToken();
                if (lexer.token == Token.OUTER) {
                    lexer.nextToken();
                }
                accept(Token.JOIN);
                joinType = wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.FULL_OUTER_JOIN;
                break;
            case INNER:
                lexer.nextToken();
                accept(Token.JOIN);
                joinType = wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.INNER_JOIN;
                break;
            case JOIN:
                lexer.nextToken();
                joinType = natural ? wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.NATURAL_JOIN : wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.JOIN;
                break;
            case COMMA:
                lexer.nextToken();
                joinType = wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.COMMA;
                break;
            case OUTER:
                lexer.nextToken();
                if (lexer.identifierEquals(FnvHash.Constants.APPLY)) {
                    lexer.nextToken();
                    joinType = wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.OUTER_APPLY;
                }
                break;
            case STRAIGHT_JOIN:
            case IDENTIFIER:
                final long hash = lexer.hash_lower;
                if (hash == FnvHash.Constants.STRAIGHT_JOIN) {
                    lexer.nextToken();
                    joinType = wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.STRAIGHT_JOIN;
                } else if (hash == FnvHash.Constants.STRAIGHT) {
                    lexer.nextToken();
                    accept(Token.JOIN);
                    joinType = wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.STRAIGHT_JOIN;
                } else if (hash == FnvHash.Constants.CROSS) {
                    lexer.nextToken();
                    if (lexer.token == Token.JOIN) {
                        lexer.nextToken();
                        joinType = natural ? wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.NATURAL_CROSS_JOIN : wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.CROSS_JOIN;
                    } else if (lexer.identifierEquals(FnvHash.Constants.APPLY)) {
                        lexer.nextToken();
                        joinType = wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.CROSS_APPLY;
                    }
                }
                break;
            default:
                break;
        }

        if (joinType != null) {
            wang.yeting.sql.ast.statement.SQLJoinTableSource join = new wang.yeting.sql.ast.statement.SQLJoinTableSource();
            join.setLeft(tableSource);
            join.setJoinType(joinType);
            join.setGlobal(global);
            if (asof) {
                join.setAsof(true);
            }

            boolean isBrace = false;
            if (wang.yeting.sql.ast.statement.SQLJoinTableSource.JoinType.COMMA == joinType) {
                if (lexer.token == Token.LBRACE) {
                    lexer.nextToken();
                    acceptIdentifier("OJ");
                    isBrace = true;
                }
            }

            wang.yeting.sql.ast.statement.SQLTableSource rightTableSource = null;
            if (lexer.token == Token.LPAREN) {
                lexer.nextToken();
                if (lexer.token == Token.SELECT
                        || (lexer.token == Token.FROM && (dbType == DbType.odps || dbType == DbType.hive))) {
                    wang.yeting.sql.ast.statement.SQLSelect select = this.select();
                    rightTableSource = new wang.yeting.sql.ast.statement.SQLSubqueryTableSource(select);
                } else {
                    rightTableSource = this.parseTableSource();
                }

                if (lexer.token == Token.UNION
                        || lexer.token == Token.EXCEPT
                        || lexer.token == Token.MINUS
                        || lexer.token == Token.INTERSECT) {
                    if (rightTableSource instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource) {
                        wang.yeting.sql.ast.statement.SQLSelect select = ((wang.yeting.sql.ast.statement.SQLSubqueryTableSource) rightTableSource).getSelect();
                        wang.yeting.sql.ast.statement.SQLSelectQuery query = queryRest(select.getQuery(), true);
                        select.setQuery(query);
                    } else if (rightTableSource instanceof wang.yeting.sql.ast.statement.SQLUnionQueryTableSource) {
                        wang.yeting.sql.ast.statement.SQLUnionQueryTableSource unionTableSrc = (wang.yeting.sql.ast.statement.SQLUnionQueryTableSource) rightTableSource;
                        unionTableSrc.setUnion((wang.yeting.sql.ast.statement.SQLUnionQuery)
                                queryRest(
                                        unionTableSrc.getUnion()
                                )
                        )
                        ;
                    }
                }

                accept(Token.RPAREN);

                if (rightTableSource instanceof wang.yeting.sql.ast.statement.SQLValuesTableSource
                        && (lexer.token == Token.AS || lexer.token == Token.IDENTIFIER)
                        && rightTableSource.getAlias() == null
                        && ((wang.yeting.sql.ast.statement.SQLValuesTableSource) rightTableSource).getColumns().size() == 0
                ) {
                    if (lexer.token == Token.AS) {
                        lexer.nextToken();
                    }
                    rightTableSource.setAlias(tableAlias(true));

                    if (lexer.token == Token.LPAREN) {
                        lexer.nextToken();
                        this.exprParser.names(((wang.yeting.sql.ast.statement.SQLValuesTableSource) rightTableSource).getColumns(), rightTableSource);
                        accept(Token.RPAREN);
                    }
                }
            } else if (lexer.token() == Token.TABLE) {
                HiveCreateTableParser createTableParser = new HiveCreateTableParser(lexer);
                HiveCreateTableStatement stmt = (HiveCreateTableStatement) createTableParser
                        .parseCreateTable(false);
                rightTableSource = new wang.yeting.sql.ast.SQLAdhocTableSource(stmt);
                primaryTableSourceRest(rightTableSource);
            } else {
                if (lexer.identifierEquals(FnvHash.Constants.UNNEST)) {
                    Lexer.SavePoint mark = lexer.mark();
                    lexer.nextToken();

                    if (lexer.token() == Token.LPAREN) {
                        lexer.nextToken();
                        wang.yeting.sql.ast.statement.SQLUnnestTableSource unnest = new wang.yeting.sql.ast.statement.SQLUnnestTableSource();
                        this.exprParser.exprList(unnest.getItems(), unnest);
                        accept(Token.RPAREN);

                        if (lexer.token() == Token.WITH) {
                            lexer.nextToken();
                            acceptIdentifier("ORDINALITY");
                            unnest.setOrdinality(true);
                        }

                        String alias = this.tableAlias();
                        unnest.setAlias(alias);

                        if (lexer.token() == Token.LPAREN) {
                            lexer.nextToken();
                            this.exprParser.names(unnest.getColumns(), unnest);
                            accept(Token.RPAREN);
                        }

                        wang.yeting.sql.ast.statement.SQLTableSource tableSrc = parseTableSourceRest(unnest);
                        rightTableSource = tableSrc;
                    } else {
                        lexer.reset(mark);
                    }
                } else if (lexer.token == Token.VALUES) {
                    rightTableSource = this.parseValues();
                }

                if (rightTableSource == null) {
                    boolean aliasToken = lexer.token == Token.LITERAL_ALIAS;
                    wang.yeting.sql.ast.SQLExpr expr;
                    switch (lexer.token) {
                        case ALL:
                            expr = this.exprParser.name();
                            break;
                        default:
                            expr = this.expr();
                            break;
                    }

                    if (aliasToken && expr instanceof wang.yeting.sql.ast.expr.SQLCharExpr) {
                        expr = new wang.yeting.sql.ast.expr.SQLIdentifierExpr(((wang.yeting.sql.ast.expr.SQLCharExpr) expr).getText());
                    }
                    wang.yeting.sql.ast.statement.SQLExprTableSource exprTableSource = new wang.yeting.sql.ast.statement.SQLExprTableSource(expr);

                    if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr && lexer.token == Token.AS) {
                        lexer.nextToken();
                        String alias = this.tableAlias(true);
                        exprTableSource.setAlias(alias);

                        if (lexer.token == Token.LPAREN) {
                            lexer.nextToken();

                            this.exprParser.names(exprTableSource.getColumns(), exprTableSource);
                            accept(Token.RPAREN);
                        }
                    }

                    rightTableSource = exprTableSource;
                }
                rightTableSource = primaryTableSourceRest(rightTableSource);
            }

            if (lexer.token == Token.USING
                    || lexer.identifierEquals(FnvHash.Constants.USING)) {
                Lexer.SavePoint savePoint = lexer.mark();
                lexer.nextToken();

                if (lexer.token == Token.LPAREN) {
                    lexer.nextToken();
                    join.setRight(rightTableSource);
                    this.exprParser.exprList(join.getUsing(), join);
                    accept(Token.RPAREN);
                } else if (lexer.token == Token.IDENTIFIER) {
                    lexer.reset(savePoint);
                    join.setRight(rightTableSource);
                    return join;
                } else {
                    join.setAlias(this.tableAlias());
                }
            } else if (lexer.token == Token.STRAIGHT_JOIN || lexer.identifierEquals(FnvHash.Constants.STRAIGHT_JOIN)) {
                primaryTableSourceRest(rightTableSource);

            } else if (rightTableSource.getAlias() == null && !(rightTableSource instanceof wang.yeting.sql.ast.statement.SQLValuesTableSource)) {
                int line = lexer.line;
                String tableAlias;
                if (lexer.token == Token.AS) {
                    lexer.nextToken();

                    if (lexer.token != Token.ON) {
                        tableAlias = this.tableAlias(true);
                    } else {
                        tableAlias = null;
                    }
                } else {
                    tableAlias = this.tableAlias(false);
                }

                if (tableAlias != null) {
                    rightTableSource.setAlias(tableAlias);

                    if (line + 1 == lexer.line
                            && lexer.hasComment()
                            && lexer.getComments().get(0).startsWith("--")) {
                        rightTableSource.addAfterComment(lexer.readAndResetComments());
                    }

                    if (lexer.token == Token.LPAREN) {
                        if (rightTableSource instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource) {
                            lexer.nextToken();
                            List<wang.yeting.sql.ast.SQLName> columns = ((wang.yeting.sql.ast.statement.SQLSubqueryTableSource) rightTableSource).getColumns();
                            this.exprParser.names(columns, rightTableSource);
                            accept(Token.RPAREN);
                        } else if (rightTableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource
                                && ((wang.yeting.sql.ast.statement.SQLExprTableSource) rightTableSource).getExpr() instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
                            List<wang.yeting.sql.ast.SQLName> columns = ((wang.yeting.sql.ast.statement.SQLExprTableSource) rightTableSource).getColumns();
                            if (columns.size() == 0) {
                                lexer.nextToken();
                                this.exprParser.names(columns, rightTableSource);
                                accept(Token.RPAREN);
                            }
                        }
                    }
                }

                rightTableSource = primaryTableSourceRest(rightTableSource);
            }

            if (lexer.token == Token.WITH) {
                lexer.nextToken();
                accept(Token.LPAREN);

                for (; ; ) {
                    wang.yeting.sql.ast.SQLExpr hintExpr = this.expr();
                    wang.yeting.sql.ast.statement.SQLExprHint hint = new wang.yeting.sql.ast.statement.SQLExprHint(hintExpr);
                    hint.setParent(tableSource);
                    rightTableSource.getHints().add(hint);
                    if (lexer.token == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    } else {
                        break;
                    }
                }

                accept(Token.RPAREN);
            }

            join.setRight(rightTableSource);

            if (!natural) {
                if (!StringUtils.isEmpty(tableSource.getAlias())
                        && tableSource.aliasHashCode64() == FnvHash.Constants.NATURAL && DbType.mysql == dbType) {
                    tableSource.setAlias(null);
                    natural = true;
                }
            }
            join.setNatural(natural);

            if (lexer.token == Token.ON) {
                lexer.nextToken();
                wang.yeting.sql.ast.SQLExpr joinOn = expr();
                join.setCondition(joinOn);

                while (lexer.token == Token.ON) {
                    lexer.nextToken();

                    wang.yeting.sql.ast.SQLExpr joinOn2 = expr();
                    join.addCondition(joinOn2);
                }

                if (dbType == DbType.odps && lexer.identifierEquals(FnvHash.Constants.USING)) {
                    wang.yeting.sql.ast.statement.SQLJoinTableSource.UDJ udj = new wang.yeting.sql.ast.statement.SQLJoinTableSource.UDJ();
                    lexer.nextToken();
                    udj.setFunction(this.exprParser.name());
                    accept(Token.LPAREN);
                    this.exprParser.exprList(udj.getArguments(), udj);
                    accept(Token.RPAREN);

                    if (lexer.token != Token.AS) {
                        udj.setAlias(alias());
                    }

                    accept(Token.AS);
                    accept(Token.LPAREN);
                    this.exprParser.names(udj.getColumns(), udj);
                    accept(Token.RPAREN);

                    if (lexer.identifierEquals(FnvHash.Constants.SORT)) {
                        lexer.nextToken();
                        accept(Token.BY);
                        this.exprParser.orderBy(udj.getSortBy(), udj);
                    }

                    if (lexer.token == Token.WITH) {
                        lexer.nextToken();
                        acceptIdentifier("UDFPROPERTIES");
                        this.exprParser.parseAssignItem(udj.getProperties(), udj);
                    }

                    join.setUdj(udj);
                }
            } else if (lexer.token == Token.USING
                    || lexer.identifierEquals(FnvHash.Constants.USING)) {
                Lexer.SavePoint savePoint = lexer.mark();
                lexer.nextToken();
                if (lexer.token == Token.LPAREN) {
                    lexer.nextToken();
                    this.exprParser.exprList(join.getUsing(), join);
                    accept(Token.RPAREN);
                } else {
                    lexer.reset(savePoint);
                }
            }

            wang.yeting.sql.ast.statement.SQLTableSource tableSourceReturn = parseTableSourceRest(join);

            if (isBrace) {
                accept(Token.RBRACE);
            }

            return parseTableSourceRest(tableSourceReturn);
        }

        if ((tableSource.aliasHashCode64() == FnvHash.Constants.LATERAL || lexer.token == Token.LATERAL)
                && lexer.token() == Token.VIEW) {
            return parseLateralView(tableSource);
        }

        if (lexer.identifierEquals(FnvHash.Constants.LATERAL) || lexer.token == Token.LATERAL) {
            lexer.nextToken();
            return parseLateralView(tableSource);
        }

        return tableSource;
    }

    public wang.yeting.sql.ast.SQLExpr expr() {
        return this.exprParser.expr();
    }

    public wang.yeting.sql.ast.SQLOrderBy parseOrderBy() {
        return this.exprParser.parseOrderBy();
    }

    public void acceptKeyword(String ident) {
        if (lexer.token == Token.IDENTIFIER && ident.equalsIgnoreCase(lexer.stringVal())) {
            lexer.nextToken();
        } else {
            setErrorEndPos(lexer.pos());
            throw new ParserException("syntax error, expect " + ident + ", actual " + lexer.token + ", " + lexer.info());
        }
    }

    public void parseFetchClause(wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock) {
        if (lexer.token == Token.LIMIT) {
            wang.yeting.sql.ast.SQLLimit limit = this.exprParser.parseLimit();
            queryBlock.setLimit(limit);
            return;
        }

        if (lexer.identifierEquals(FnvHash.Constants.OFFSET) || lexer.token == Token.OFFSET) {
            lexer.nextToken();
            wang.yeting.sql.ast.SQLExpr offset = this.exprParser.expr();
            queryBlock.setOffset(offset);
            if (lexer.identifierEquals(FnvHash.Constants.ROW) || lexer.identifierEquals(FnvHash.Constants.ROWS)) {
                lexer.nextToken();
            }
        }

        if (lexer.token == Token.FETCH) {
            lexer.nextToken();
            if (lexer.token == Token.FIRST
                    || lexer.token == Token.NEXT
                    || lexer.identifierEquals(FnvHash.Constants.NEXT)) {
                lexer.nextToken();
            } else {
                acceptIdentifier("FIRST");
            }
            wang.yeting.sql.ast.SQLExpr first = this.exprParser.primary();
            queryBlock.setFirst(first);
            if (lexer.identifierEquals(FnvHash.Constants.ROW) || lexer.identifierEquals(FnvHash.Constants.ROWS)) {
                lexer.nextToken();
            }

            if (lexer.token == Token.ONLY) {
                lexer.nextToken();
            } else {
                acceptIdentifier("ONLY");
            }
        }
    }

    protected void parseHierachical(wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock) {
        if (lexer.token == Token.CONNECT || lexer.identifierEquals(FnvHash.Constants.CONNECT)) {
            lexer.nextToken();
            accept(Token.BY);

            if (lexer.token == Token.PRIOR || lexer.identifierEquals(FnvHash.Constants.PRIOR)) {
                lexer.nextToken();
                queryBlock.setPrior(true);
            }

            if (lexer.identifierEquals(FnvHash.Constants.NOCYCLE)) {
                queryBlock.setNoCycle(true);
                lexer.nextToken();

                if (lexer.token == Token.PRIOR) {
                    lexer.nextToken();
                    queryBlock.setPrior(true);
                }
            }
            queryBlock.setConnectBy(this.exprParser.expr());
        }

        if (lexer.token == Token.START || lexer.identifierEquals(FnvHash.Constants.START)) {
            lexer.nextToken();
            accept(Token.WITH);

            queryBlock.setStartWith(this.exprParser.expr());
        }

        if (lexer.token == Token.CONNECT || lexer.identifierEquals(FnvHash.Constants.CONNECT)) {
            lexer.nextToken();
            accept(Token.BY);

            if (lexer.token == Token.PRIOR || lexer.identifierEquals(FnvHash.Constants.PRIOR)) {
                lexer.nextToken();
                queryBlock.setPrior(true);
            }

            if (lexer.identifierEquals(FnvHash.Constants.NOCYCLE)) {
                queryBlock.setNoCycle(true);
                lexer.nextToken();

                if (lexer.token == Token.PRIOR || lexer.identifierEquals(FnvHash.Constants.PRIOR)) {
                    lexer.nextToken();
                    queryBlock.setPrior(true);
                }
            }
            queryBlock.setConnectBy(this.exprParser.expr());
        }
    }

    protected wang.yeting.sql.ast.statement.SQLTableSource parseLateralView(wang.yeting.sql.ast.statement.SQLTableSource tableSource) {
        accept(Token.VIEW);
        if (tableSource != null && "LATERAL".equalsIgnoreCase(tableSource.getAlias())) {
            tableSource.setAlias(null);
        }
        wang.yeting.sql.ast.statement.SQLLateralViewTableSource lateralViewTabSrc = new wang.yeting.sql.ast.statement.SQLLateralViewTableSource();
        lateralViewTabSrc.setTableSource(tableSource);

        if (lexer.token == Token.OUTER) {
            lateralViewTabSrc.setOuter(true);
            lexer.nextToken();
        }

        wang.yeting.sql.ast.expr.SQLMethodInvokeExpr udtf = (wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) this.exprParser.primary();
        lateralViewTabSrc.setMethod(udtf);

        String alias;
        if (lexer.token == Token.AS) {
            lexer.nextToken();
            if (lexer.token == Token.AS) {
                lexer.nextToken();
            }

            alias = alias();
        } else {
            alias = as();
        }
        if (alias != null) {
            lateralViewTabSrc.setAlias(alias);
        }

        if (lexer.token == Token.AS) {
            parseLateralViewAs(lateralViewTabSrc);
        }

        if (lexer.token == Token.ON) {
            lexer.nextToken();
            lateralViewTabSrc.setOn(
                    this.exprParser.expr()
            );
        }

        return parseTableSourceRest(lateralViewTabSrc);
    }

    public void parseLateralViewAs(wang.yeting.sql.ast.statement.SQLLateralViewTableSource lateralViewTabSrc) {
        accept(Token.AS);

        Lexer.SavePoint mark = null;
        for (; ; ) {
            wang.yeting.sql.ast.SQLName name;
            if (lexer.token == Token.NULL) {
                name = new wang.yeting.sql.ast.expr.SQLIdentifierExpr(lexer.stringVal());
                lexer.nextToken();
            } else {
                name = this.exprParser.name();
                if (name instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                    lexer.reset(mark);
                    break;
                }
            }
            name.setParent(lateralViewTabSrc);
            lateralViewTabSrc.getColumns().add(name);
            if (lexer.token == Token.COMMA) {
                mark = lexer.mark();
                lexer.nextToken();
                continue;
            }
            break;
        }
    }

    public wang.yeting.sql.ast.statement.SQLValuesTableSource parseValues() {
        accept(Token.VALUES);
        wang.yeting.sql.ast.statement.SQLValuesTableSource tableSource = new wang.yeting.sql.ast.statement.SQLValuesTableSource();

        for (; ; ) {

            // compatible (VALUES 1,2,3) and (VALUES (1), (2), (3)) for ads
            boolean isSingleValue = true;
            if (lexer.token == Token.ROW) {
                lexer.nextToken();
            }
            if (lexer.token() == Token.LPAREN) {
                accept(Token.LPAREN);
                isSingleValue = false;
            }

            wang.yeting.sql.ast.expr.SQLListExpr listExpr = new wang.yeting.sql.ast.expr.SQLListExpr();

            if (isSingleValue) {
                wang.yeting.sql.ast.SQLExpr expr = expr();
                expr.setParent(listExpr);
                listExpr.getItems().add(expr);
            } else {
                this.exprParser.exprList(listExpr.getItems(), listExpr);
                accept(Token.RPAREN);
            }

            listExpr.setParent(tableSource);

            tableSource.getValues().add(listExpr);

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }

        String alias = this.tableAlias();
        if (alias != null) {
            tableSource.setAlias(alias);
        }

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            this.exprParser.names(tableSource.getColumns(), tableSource);
            accept(Token.RPAREN);
        }


        return tableSource;
    }
}
