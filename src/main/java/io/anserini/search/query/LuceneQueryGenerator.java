package io.anserini.search.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;

public class LuceneQueryGenerator extends QueryGenerator {
    @Override
    public Query buildQuery(String field, Analyzer analyzer, String queryText) {
        try {
            return new StandardQueryParser(analyzer).parse(queryText, field);
        } catch (QueryNodeException e) {
            System.err.println("parsing query error" + queryText);
            return null;
        }
    }
}