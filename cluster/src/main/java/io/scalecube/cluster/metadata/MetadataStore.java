package io.scalecube.cluster.metadata;

import java.util.Map;

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
   * @param metadata local member metadata
   */
  void updateMetadata(Map<String, String> metadata);
}
