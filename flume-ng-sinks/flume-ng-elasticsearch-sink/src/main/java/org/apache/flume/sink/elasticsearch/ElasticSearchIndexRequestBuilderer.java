package org.apache.flume.sink.elasticsearch;

import java.io.IOException;

import org.apache.flume.Event;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.io.BytesStream;

/**
 * Interface for preparing ElasticSearch {@link IndexRequestBuilder}
 * instances from serialized flume events.
 */
public interface ElasticSearchIndexRequestBuilderer {

  /**
   * @param indexRequestBuilder
   *          ElasticSearch {@link IndexRequestBuilder} to prepare
   * @param indexName
   *          Index name to use -- as configured on the sink
   * @param indexType
   *          Index type to use -- as configured on the sink
   * @param event
   *          Flume event to serialize and index
   * @throws IOException
   *           If an error occurs e.g. during serialization
   */
  void prepareIndexRequest(IndexRequestBuilder indexRequestBuilder,
      String indexName, String indexType, Event event) throws IOException;

}

/**
 * Default implementation of {@link ElasticSearchIndexRequestBuilderer}.
 * It serializes the flume event using the
 * {@link ElasticSearchEventSerializer} instance configured on the sink.
 */
class EventSerializerIndexRequestBuilderer
  implements ElasticSearchIndexRequestBuilderer {

  private final ElasticSearchEventSerializer serializer;

  EventSerializerIndexRequestBuilderer(
      ElasticSearchEventSerializer serializer) {
    this.serializer = serializer;
  }

  @Override
  public void prepareIndexRequest(IndexRequestBuilder indexRequestBuilder,
      String indexName, String indexType, Event event) throws IOException {
    BytesStream contentBuilder = serializer.getContentBuilder(event);
    indexRequestBuilder.setIndex(indexName).setType(indexType)
      .setSource(contentBuilder.bytes());
  }

}
