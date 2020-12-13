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

package io.anserini.ltr.feature.base;

import io.anserini.index.IndexArgs;
import io.anserini.ltr.feature.DocumentContext;
import io.anserini.ltr.feature.FieldContext;
import io.anserini.ltr.feature.FeatureExtractor;
import io.anserini.ltr.feature.QueryContext;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * Counts all unordered pairs of query tokens
 */
public class UnorderedQueryPairs implements FeatureExtractor {
  private String field;

  private int gapSize = 8;

  public UnorderedQueryPairs() {
    this.field = IndexArgs.CONTENTS;
  }

  public UnorderedQueryPairs(int gapSize) {
    this.gapSize = gapSize;
    this.field = IndexArgs.CONTENTS;
  }

  public UnorderedQueryPairs(int gapSize, String field) {
    this.gapSize = gapSize;
    this.field = field;
  }

  public float extract(DocumentContext documentContext, QueryContext queryContext) {
    FieldContext context = documentContext.fieldContexts.get(field);
    float count = 0;
    List<Pair<String, String>> queryPairs= queryContext.genQueryPair();
    for(Pair<String, String> pair: queryPairs){
      count += context.countBigram(pair.getLeft(),pair.getRight(),gapSize);
      count += context.countBigram(pair.getRight(),pair.getLeft(),gapSize);
    }
    return count;
  }

  @Override
  public float postEdit(DocumentContext context, QueryContext queryContext) {
    return queryContext.getSelfLog(context.docId, getName());
  }

  @Override
  public String getName() {
    return String.format("%s_UnorderedQueryPairs_%d", field, this.gapSize);
  }

  @Override
  public String getField() {
    return field;
  }

  @Override
  public FeatureExtractor clone() {
    return new UnorderedQueryPairs(gapSize, field);
  }
}
