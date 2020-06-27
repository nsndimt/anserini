/*
 * Anserini: A Lucene toolkit for replicable information retrieval research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.anserini.ltr;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.analysis.DefaultEnglishAnalyzer;
import io.anserini.index.IndexArgs;
import io.anserini.ltr.feature.FeatureExtractors;
import io.anserini.ltr.feature.OrderedSequentialPairsFeatureExtractor;
import io.anserini.ltr.feature.UnigramFeatureExtractor;
import io.anserini.ltr.feature.UnorderedSequentialPairsFeatureExtractor;
import io.anserini.ltr.feature.base.*;
import io.anserini.rerank.RerankerContext;
import io.anserini.search.query.BagOfWordsQueryGenerator;
import io.anserini.search.query.QueryGenerator;
import io.anserini.util.Qrels;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;
import java.util.*;

/**
 * Feature extractor for the gov two collection
 */
public class CovidFeatureExtractor extends BaseFeatureExtractor {
  private static final Logger LOG = LogManager.getLogger(CovidFeatureExtractor.class);

  //**************************************************
  //**************************************************
  private static final FeatureExtractors DEFAULT_EXTRACTOR_CHAIN = FeatureExtractors.
          createFeatureExtractorChain(new UnigramFeatureExtractor(),
                  new UnorderedSequentialPairsFeatureExtractor(6),
                  new UnorderedSequentialPairsFeatureExtractor(8),
                  new UnorderedSequentialPairsFeatureExtractor(10),
                  new OrderedSequentialPairsFeatureExtractor(6),
                  new OrderedSequentialPairsFeatureExtractor(8),
                  new OrderedSequentialPairsFeatureExtractor(10),
                  new MatchingTermCount(),
                  new QueryLength(),
                  new SumMatchingTf(),
                  new TermFrequencyFeatureExtractor(),
                  new BM25FeatureExtractor(),
                  new TFIDFFeatureExtractor(),
                  new UniqueTermCount(),
                  new DocSizeFeatureExtractor(),
                  new AvgICTFFeatureExtractor(),
                  new AvgIDFFeatureExtractor(),
                  new SimplifiedClarityFeatureExtractor(),
                  new PMIFeatureExtractor(),
                  new SCQFeatureExtractor()
          );

  //**************************************************
  //**************************************************

  private QueryGenerator parser;

  public CovidFeatureExtractor(IndexReader reader, Qrels qrels, Map<String, Map<String, String>> topics,
                               String topic_field) {
    this(reader, qrels, topics, topic_field, getDefaultExtractors());
    LOG.debug("Web Feature extractor initialized.");
  }

  /**
   * FeatureExtractor constructor requires an index reader, qrels, and topics
   * also takes in optional customExtractors, if null, the default will be used
   * @param reader
   * @param qrels
   * @param topics
   * @param customExtractors
   */
  @SuppressWarnings("unchecked")
  public <K> CovidFeatureExtractor(IndexReader reader, Qrels qrels, Map<K, Map<String, String>> topics,
                                   String topic_field, FeatureExtractors customExtractors) {
    super(reader, qrels, topics, topic_field, customExtractors == null ? getDefaultExtractors() : customExtractors);
    this.parser = new BagOfWordsQueryGenerator();

    LOG.debug("Web Feature extractor initialized.");
  }

  @Override
  protected String getIdField() {
    return IndexArgs.ID;
  }

  @Override
  protected String getTermVectorField() {
    return IndexArgs.CONTENTS;
  }

  public static FeatureExtractors getDefaultExtractors() {
    return DEFAULT_EXTRACTOR_CHAIN;
  }

  @Override
  protected Analyzer getAnalyzer() {
    return DefaultEnglishAnalyzer.newDefaultInstance();
  }

  @Override
  protected Set<String> getFieldsToLoad() {
    return new HashSet<>(Arrays.asList(getIdField(), getTermVectorField()));
  }

  @Override
  protected Query parseQuery(String queryText) {
    return this.parser.buildQuery(getTermVectorField(),getAnalyzer(),queryText);
  }

  @Override
  protected Query docIdQuery(String docId) {
    return new TermQuery(new Term(getIdField(), docId));
  }

}
