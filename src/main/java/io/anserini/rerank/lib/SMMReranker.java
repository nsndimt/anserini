package io.anserini.rerank.lib;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.index.IndexArgs;
import io.anserini.rerank.Reranker;
import io.anserini.rerank.RerankerContext;
import io.anserini.rerank.ScoredDocuments;
import io.anserini.util.FeatureVector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.*;

import static io.anserini.search.SearchCollection.BREAK_SCORE_TIES_BY_DOCID;
import static io.anserini.search.SearchCollection.BREAK_SCORE_TIES_BY_TWEETID;

public class SMMReranker implements Reranker {
    private static final Logger LOG = LogManager.getLogger(Rm3Reranker.class);

    private final Analyzer analyzer;
    private final String field;

    private final int fbTerms;
    private final int fbDocs;
    private final float originalQueryWeight;
    private final float lambda;
    private final boolean outputQuery;

    public SMMReranker(Analyzer analyzer, String field, int fbTerms, int fbDocs, float originalQueryWeight, float lambda, boolean outputQuery) {
        this.analyzer = analyzer;
        this.field = field;
        this.fbTerms = fbTerms;
        this.fbDocs = fbDocs;
        this.originalQueryWeight = originalQueryWeight;
        this.lambda = lambda;
        this.outputQuery = outputQuery;
    }

    @Override
    public ScoredDocuments rerank(ScoredDocuments docs, RerankerContext context) {
        assert(docs.documents.length == docs.scores.length);

        IndexSearcher searcher = context.getIndexSearcher();
        IndexReader reader = searcher.getIndexReader();

        FeatureVector qfv = FeatureVector.fromTerms(AnalyzerUtils.analyze(analyzer, context.getQueryText())).scaleToUnitL1Norm();

        boolean useRf = (context.getSearchArgs().rf_qrels != null);
        FeatureVector rm = estimateRelevanceModel(docs, qfv.getFeatures(), reader, useRf);

        rm = FeatureVector.interpolate(qfv, rm, originalQueryWeight);

        BooleanQuery.Builder feedbackQueryBuilder = new BooleanQuery.Builder();

        Iterator<String> terms = rm.iterator();
        while (terms.hasNext()) {
            String term = terms.next();
            float prob = rm.getFeatureWeight(term);
            feedbackQueryBuilder.add(new BoostQuery(new TermQuery(new Term(field, term)), prob), BooleanClause.Occur.SHOULD);
        }

        Query feedbackQuery = feedbackQueryBuilder.build();

        if (outputQuery) {
            LOG.info("QID: " + context.getQueryId());
            LOG.info("Original Query: " + context.getQuery().toString(field));
            LOG.info("Running new query: " + feedbackQuery.toString(field));
        }

        TopDocs rs;
        try {
            Query finalQuery = feedbackQuery;
            // If there's a filter condition, we need to add in the constraint.
            // Otherwise, just use the feedback query.
            if (context.getFilter() != null) {
                BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
                bqBuilder.add(context.getFilter(), BooleanClause.Occur.FILTER);
                bqBuilder.add(feedbackQuery, BooleanClause.Occur.MUST);
                finalQuery = bqBuilder.build();
            }

            // Figure out how to break the scoring ties.
            if (context.getSearchArgs().arbitraryScoreTieBreak) {
                rs = searcher.search(finalQuery, context.getSearchArgs().hits);
            } else if (context.getSearchArgs().searchtweets) {
                rs = searcher.search(finalQuery, context.getSearchArgs().hits, BREAK_SCORE_TIES_BY_TWEETID, true);
            } else {
                rs = searcher.search(finalQuery, context.getSearchArgs().hits, BREAK_SCORE_TIES_BY_DOCID, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return docs;
        }

        return ScoredDocuments.fromTopDocs(rs, searcher);
    }

    private FeatureVector estimateRelevanceModel(ScoredDocuments docs, Set<String> ori_q, IndexReader reader, boolean useRf) {
        int numdocs;
        if (useRf) {
            numdocs = docs.documents.length;
        }
        else {
            numdocs = docs.documents.length < fbDocs ? docs.documents.length : fbDocs;
        }

        FeatureVector rel_freq = new FeatureVector();
        FeatureVector bg_p = new FeatureVector();

        for (int i = 0; i < numdocs; i++) {
            if (useRf && docs.scores[i] <= .0) {
                continue;
            }
            try {
                Terms terms = reader.getTermVector(docs.ids[i], field);
                int numDocs = reader.numDocs();
                TermsEnum termsEnum = terms.iterator();

                BytesRef text;
                while ((text = termsEnum.next()) != null) {
                    String term = text.utf8ToString();

                    if (term.length() < 2 || term.length() > 20) continue;
                    if (!term.matches("[a-z0-9]+")) continue;
                    if (term.matches("[0-9]+")) continue;

                    int df = reader.docFreq(new Term(IndexArgs.CONTENTS, term));

                    float ratio = (float) df / numDocs;
                    if (ratio > 0.1f) continue;

                    int freq = (int) termsEnum.totalTermFreq();
                    rel_freq.addFeatureWeight(term, (float) freq);
                }

            } catch (IOException e) {
                e.printStackTrace();
                // Just return empty feature vector.
                return new FeatureVector();
            }
        }
        float total_rel_terms = (float) rel_freq.computeL1Norm();
        FeatureVector f = new FeatureVector();

        try {
            Long total_terms = reader.getSumTotalTermFreq(field);
            for(String term: rel_freq.getFeatures()){
                Long term_freq = reader.totalTermFreq(new Term(field, term));
                bg_p.addFeatureWeight(term, ((float)term_freq)/total_terms);
                f.addFeatureWeight(term,rel_freq.getFeatureWeight(term)/total_rel_terms);
            }
        } catch (IOException e) {
            e.printStackTrace();
            // Just return empty feature vector.
            return new FeatureVector();
        }

        float log_loss = 0;
        float pre_log_loss;

        for(String term: rel_freq.getFeatures()){
            log_loss += rel_freq.getFeatureWeight(term) *
                    Math.log((1.0 - lambda) * f.getFeatureWeight(term) + lambda * bg_p.getFeatureWeight(term));
        }

        int num_iter = 0;
        do {
            FeatureVector temp = new FeatureVector();
            for(String term: rel_freq.getFeatures()){
                double t_w = ((1.0 - lambda) * f.getFeatureWeight(term))/
                        ((1.0 -lambda) * f.getFeatureWeight(term) + lambda * bg_p.getFeatureWeight(term));
                float p_w = (float)(rel_freq.getFeatureWeight(term) * t_w);
                temp.addFeatureWeight(term, p_w);
            }
            temp.scaleToUnitL1Norm();
            f = temp;

            pre_log_loss = log_loss;
            log_loss = 0;
            for(String term: rel_freq.getFeatures()){
                log_loss += rel_freq.getFeatureWeight(term) *
                        Math.log((1.0 - lambda) * f.getFeatureWeight(term) +
                                lambda * bg_p.getFeatureWeight(term));
            }
            num_iter++;
        } while((Math.abs(pre_log_loss-log_loss)>1e-6)|| num_iter>1000);

        f.pruneToSize(fbTerms);
        f.scaleToUnitL1Norm();

        return f;
    }

    @Override
    public String tag() {
        return "smm(fbDocs="+fbDocs+",fbTerms="+fbTerms+",originalQueryWeight:"+originalQueryWeight+",lambda:"+lambda+")";
    }
}
