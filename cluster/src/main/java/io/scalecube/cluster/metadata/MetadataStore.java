package io.scalecube.cluster.metadata;

import io.scalecube.cluster.Member;
import java.util.Map;
import reactor.core.publisher.Mono;

/**
 * Cluster component for hosting members metadata as well functions operating over local member
 * metadata.
 */
public interface MetadataStore {

  /** Start listening on requests on getting local member metadata to remote callers. */
  void start();

  /** Stop listening on requests and dispose resporces. */
  void stop();

  /**
   * Returns local cluster member metadata. Never null.
   *
   * @return local member metadata
   */
  Map<String, String> metadata();

  /**
   * Returns cluster member metadata. Null if member was removed.
   *
   * @param member cluster member
   * @return metadata of the cluster member
   */
  Map<String, String> metadata(Member member);

  /**
   * Updates local cluster member metadata.
   *
   * @param metadata local member metadata
   */
  void updateMetadata(Map<String, String> metadata);

  /**
   * Updates given cluster member with its corresponding metadata.
   *
   * @param member member
   * @param metadata cluster member metadtaa
   */
  void updateMetadata(Member member, Map<String, String> metadata);

  /**
   * Retrives cluster member metadata remotely.
   *
   * @param member cluster member
   * @return mono result of getting remote member metadata
   */
  Mono<Map<String, String>> fetchMetadata(Member member);

  /**
   * Removes cluster member metadata from store.
   *
   * @param member cluster member
   */
  void removeMetadata(Member member);
}
