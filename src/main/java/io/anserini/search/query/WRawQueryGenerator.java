package io.anserini.search.query;

import io.anserini.analysis.AnalyzerUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import java.util.ArrayList;
import java.util.List;

public class WRawQueryGenerator extends QueryGenerator {
  @Override
  public Query buildQuery(String field, Analyzer analyzer, String queryText) {
    String[] weightedTerms = queryText.split(" ");

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (String s : weightedTerms) {
      String[] wt = s.split(":");
      assert wt.length == 2;
      float weight = Float.parseFloat(wt[0]);
      String term = wt[1];
      builder.add(new BoostQuery(new TermQuery(new Term(field, term)), weight), BooleanClause.Occur.SHOULD);
    }

    return builder.build();
  }

  public List<String> parse(String field, Analyzer analyzer, String queryText) {
    String[] weightedTerms = queryText.split(" ");
    List<String> queryTokens = new ArrayList<>();
    for (String s : weightedTerms) {
      String[] wt = s.split(":");
      assert wt.length == 2;
      float weight = Float.parseFloat(wt[0]);
      String term = wt[1];
      queryTokens.add(term);
    }

    return queryTokens;
  }
}