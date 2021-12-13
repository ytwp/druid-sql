package wang.yeting.sql.dialect.presto.parser;

import wang.yeting.sql.DbType;
import wang.yeting.sql.parser.Keywords;
import wang.yeting.sql.parser.Lexer;
import wang.yeting.sql.parser.SQLParserFeature;
import wang.yeting.sql.parser.Token;

import java.util.HashMap;
import java.util.Map;

public class PrestoLexer extends Lexer {
    public final static Keywords DEFAULT_PHOENIX_KEYWORDS;

    static {
        Map<String, Token> map = new HashMap<String, Token>();

        map.putAll(Keywords.DEFAULT_KEYWORDS.getKeywords());

        map.put("FETCH", Token.FETCH);
        map.put("FIRST", Token.FIRST);
        map.put("ONLY", Token.ONLY);
        map.put("OPTIMIZE", Token.OPTIMIZE);
        map.put("OF", Token.OF);
        map.put("CONCAT", Token.CONCAT);
        map.put("CONTINUE", Token.CONTINUE);
        map.put("IDENTITY", Token.IDENTITY);
        map.put("MERGE", Token.MERGE);
        map.put("USING", Token.USING);
        map.put("MATCHED", Token.MATCHED);
        map.put("UPSERT", Token.UPSERT);
        map.put("ARRAY", Token.ARRAY);

        DEFAULT_PHOENIX_KEYWORDS = new Keywords(map);
    }

    {
        dbType = DbType.presto;
    }

    public PrestoLexer(String input, SQLParserFeature... features) {
        super(input);
        super.keywords = DEFAULT_PHOENIX_KEYWORDS;
        for (SQLParserFeature feature : features) {
            config(feature, true);
        }
    }
}

