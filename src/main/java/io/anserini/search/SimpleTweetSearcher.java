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

package io.anserini.search;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.analysis.TweetAnalyzer;
import io.anserini.index.IndexArgs;
import io.anserini.index.generator.TweetGenerator;
import io.anserini.rerank.RerankerContext;
import io.anserini.rerank.ScoredDocuments;
import io.anserini.search.query.BagOfWordsQueryGenerator;
import io.anserini.search.query.QueryGenerator;
import io.anserini.search.query.WRawQueryGenerator;
import io.anserini.search.topicreader.JsonTopicReader;
import io.anserini.search.topicreader.TopicReader;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.kohsuke.args4j.ParserProperties;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class that exposes basic search functionality, designed specifically to provide the bridge between Java and Python
 * via pyjnius.
 */
public class SimpleTweetSearcher extends SimpleSearcher implements Closeable {
  public static final Sort BREAK_SCORE_TIES_BY_DATE =
      new Sort(SortField.FIELD_SCORE,
          new SortField("date", SortField.Type.LONG, true));
  private static final Logger LOG = LogManager.getLogger(SimpleTweetSearcher.class);

  public static final class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "Path to Lucene index.")
    public String index;

    @Option(name = "-bm25", usage = "Flag to use BM25.", forbids = {"-ql"})
    public Boolean useBM25 = true;

    @Option(name = "-bm25.k1", usage = "BM25 k1 value.", forbids = {"-ql"})
    public float bm25_k1 = 0.9f;

    @Option(name = "-bm25.b", usage = "BM25 b value.", forbids = {"-ql"})
    public float bm25_b = 0.4f;

    @Option(name = "-qld", usage = "Flag to use query-likelihood with Dirichlet smoothing.", forbids={"-bm25"})
    public Boolean useQL = false;

    @Option(name = "-qld.mu", usage = "Dirichlet smoothing parameter value for query-likelihood.", forbids={"-bm25"})
    public float ql_mu = 1000.0f;

    @Option(name = "-topics", metaVar = "[file]", required = true, usage = "Topics file.")
    public String topics;

    @Option(name = "-output", metaVar = "[file]", required = true, usage = "Output run file.")
    public String output;

    @Option(name = "-hits", metaVar = "[number]", usage = "max number of hits to return")
    public int hits = 1000;

    @Option(name = "-threads", metaVar = "[number]", usage = "Number of threads to use.")
    public int threads = 1;
  }

  protected SimpleTweetSearcher() {
  }

  public SimpleTweetSearcher(String indexDir) throws IOException {
    super(indexDir);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  public Map<String, Result[]> batchSearchTweet(List<Pair<String, Long>> queries, List<String> qids, int k, int threads) {
    // Create the IndexSearcher here, if needed. We do it here because if we leave the creation to the search
    // method, we might end up with a race condition as multiple threads try to concurrently create the IndexSearcher.
    if (searcher == null) {
      searcher = new IndexSearcher(reader);
      searcher.setSimilarity(similarity);
    }

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads);
    ConcurrentHashMap<String, Result[]> results = new ConcurrentHashMap<>();

    long startTime = System.nanoTime();
    AtomicLong index = new AtomicLong();
    int queryCnt = queries.size();
    for (int q = 0; q < queryCnt; ++q) {
      String qid = qids.get(q);
      String queryExp = queries.get(q).getLeft();
      long timeFilter = queries.get(q).getRight();
      executor.execute(() -> {
        try {
          WRawQueryGenerator parser = new WRawQueryGenerator();
          Query query = parser.buildQuery(IndexArgs.CONTENTS, analyzer, queryExp);

          SearchArgs searchArgs = new SearchArgs();
          searchArgs.arbitraryScoreTieBreak = false;
          searchArgs.hits = k;
          searchArgs.searchtweets = true;

          TopDocs rs;

          // Do not consider the tweets with tweet ids that are beyond the queryTweetTime
          // <querytweettime> tag contains the timestamp of the query in terms of the
          // chronologically nearest tweet id within the corpus
          Query filter = LongPoint.newRangeQuery("date", 0L, timeFilter);
          BooleanQuery.Builder builder = new BooleanQuery.Builder();
          builder.add(filter, BooleanClause.Occur.FILTER);
          builder.add(query, BooleanClause.Occur.MUST);
          Query compositeQuery = builder.build();
          rs = searcher.search(compositeQuery, k, BREAK_SCORE_TIES_BY_DATE, true);
          ScoredDocuments hits = ScoredDocuments.fromTopDocs(rs, searcher);
          Result[] rankList = new Result[hits.ids.length];
          for (int i = 0; i < hits.ids.length; i++) {
            Document doc = hits.documents[i];
            String docid = doc.getField(IndexArgs.ID).stringValue();

            IndexableField field;
            field = doc.getField(IndexArgs.CONTENTS);
            String contents = field == null ? null : field.stringValue();

            field = doc.getField(IndexArgs.RAW);
            String raw = field == null ? null : field.stringValue();
            rankList[i] = new Result(docid, hits.ids[i], hits.scores[i], contents, raw, doc);
          }
          results.put(qid, rankList);

        } catch (IOException e) {
          throw new CompletionException(e);
        }
        // logging for speed
        Long lineNumber = index.incrementAndGet();
        if (lineNumber % 100 == 0) {
          double timePerQuery = (double) (System.nanoTime() - startTime) / (lineNumber + 1) / 1e9;
          LOG.info(String.format("Retrieving query " + lineNumber + " (%.3f s/query)", timePerQuery));
        }
      });
    }

    executor.shutdown();

    try {
      // Wait for existing tasks to terminate
      while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.info(String.format("%.2f percent completed",
                (double) executor.getCompletedTaskCount() / queries.size() * 100.0d));
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      executor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }

    if (queryCnt != executor.getCompletedTaskCount()) {
      throw new RuntimeException("queryCount = " + queryCnt +
              " is not equal to completedTaskCount =  " + executor.getCompletedTaskCount());
    }

    return results;
  }

  // Note that this class is primarily meant to be used by automated regression scripts, not humans!
  // tl;dr - Do not use this class for running experiments. Use SearchCollection instead!
  //
  // SimpleTweetSearcher is the main class that exposes search functionality for Pyserini (in Python).
  // As such, it has a different code path than SearchCollection, the preferred entry point for running experiments
  // from Java. The main method here exposes only barebone options, primarily designed to verify that results from
  // SimpleSearcher are *exactly* the same as SearchCollection (e.g., via automated regression scripts).
  public static void main(String[] args) throws Exception {
    Args searchArgs = new Args();
    CmdLineParser parser = new CmdLineParser(searchArgs, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println("Example: SimpleTweetSearcher" + parser.printExample(OptionHandlerFilter.REQUIRED));
      return;
    }

    final long start = System.nanoTime();
    SimpleTweetSearcher searcher = new SimpleTweetSearcher(searchArgs.index);
    List<String> argsAsList = Arrays.asList(args);
    if (argsAsList.contains("-bm25")) {
      LOG.info("Testing code path of explicitly setting BM25.");
      searcher.setBM25(searchArgs.bm25_k1, searchArgs.bm25_b);
    } else if (searchArgs.useQL){
      LOG.info("Testing code path of explicitly setting QL.");
      searcher.setQLD(searchArgs.ql_mu);
    }
    SortedMap<String, Map<String, String>> topics = new JsonTopicReader(Paths.get(searchArgs.topics)).read();

    PrintWriter out = new PrintWriter(Files.newBufferedWriter(Paths.get(searchArgs.output), StandardCharsets.US_ASCII));

    List<Pair<String, Long>> taskInput = new ArrayList<>();
    List<String> taskIds = new ArrayList<>();
    for (String id : topics.keySet()) {
      taskIds.add(id);
      long t = Long.parseLong(topics.get(id).get("date"));
      taskInput.add(Pair.of(topics.get(id).get("title"), t));
    }
    Map<String, Result[]> threadResult = searcher.batchSearchTweet(taskInput, taskIds, searchArgs.hits, searchArgs.threads);
    for (String id : taskIds) {
      Result[] results = threadResult.get(id);
      for (int i=0; i<results.length; i++) {
        out.println(String.format(Locale.US, "%s Q0 %s %d %f Anserini",
            id, results[i].docid, (i+1), results[i].score));
      }
    }
    out.close();

    final long durationMillis = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    LOG.info("Total run time: " + DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss"));
  }
}
