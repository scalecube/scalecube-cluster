package io.scalecube.cluster.metadata;

import java.util.Map;
import reactor.core.publisher.Mono;

public interface MetadataStore {

  /**
   * Returns local cluster member metadata.
   *
   * @return local member metadata
   */
  Map<String, String> metadata();

  /**
   * Updates local member metadata.
   *
   * @param metadata metadata
   * @return mono handle
   */
  Mono<Void> updateMetadata(Map<String, String> metadata);
}
