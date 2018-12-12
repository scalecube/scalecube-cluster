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
   * Returns local cluster member metadata. In case of absence of local metadata simply empty map
   * will be returned.
   *
   * @return local member metadata
   */
  Map<String, String> metadata();

  /**
   * Returns cluster member metadata. May or may not result in remote call.
   *
   * @param member cluster member
   * @return metadata of the cluster member
   */
  Mono<Map<String, String>> metadata(Member member);

  /**
   * Updates local member metadata.
   *
   * @param metadata local member metadata
   */
  void updateMetadata(Map<String, String> metadata);
}
