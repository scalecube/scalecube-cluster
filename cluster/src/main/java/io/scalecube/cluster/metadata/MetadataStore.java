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
   * Returns local cluster member metadata from local store. Never null.
   *
   * @return local member metadata
   */
  Map<String, String> metadata();

  /**
   * Returns cluster member metadata from local store. Null if member was removed.
   *
   * @param member cluster member
   * @return metadata of the cluster member
   */
  Map<String, String> metadata(Member member);

  /**
   * Updates local cluster member metadata locally. Shortcut method for {@link
   * #updateMetadata(Member, Map)}.
   *
   * @param metadata local member metadata
   * @return old metadata or null
   */
  Map<String, String> updateMetadata(Map<String, String> metadata);

  /**
   * Updates cluster member metadata locally.
   *
   * @param member member
   * @param metadata cluster member metadtaa
   * @return old metadata or null
   */
  Map<String, String> updateMetadata(Member member, Map<String, String> metadata);

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
   * @param member cluster member (local member is not allowed here)
   * @return old metadata or null
   */
  Map<String, String> removeMetadata(Member member);
}
