package io.anserini.search.topicreader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class JsonTopicReader extends TopicReader<String> {
  public JsonTopicReader(Path topicFile) {
    super(topicFile);
  }

  @Override
  public SortedMap<String, Map<String, String>> read(BufferedReader reader) throws IOException {
    SortedMap<String, Map<String, String>> map = new TreeMap<>();

    String line;
    while ((line = reader.readLine()) != null) {
      line = line.trim();
      ObjectMapper mapper = new ObjectMapper();
      JsonNode node = mapper.readTree(line);

      Map<String,String> fields = new HashMap<>();
      fields.put("title", node.get("title").asText());
      fields.put("date", node.get("date").asText());
      map.put(node.get("qid").asText(), fields);
    }

    return map;
  }
}
