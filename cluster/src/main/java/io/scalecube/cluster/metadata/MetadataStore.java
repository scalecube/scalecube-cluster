package io.scalecube.cluster.metadata;

import io.scalecube.cluster.Member;
import java.util.Map;
import reactor.core.publisher.Mono;

/**
 * Cluster component for hosting members metadata as well functions operating over local member
 * metadata.
 */
public interface MetadataStore {

  String INCARNATION_KEY = "incarnation";

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

  Integer metadataVersion(Member member);

  /**
   * Updates local cluster member metadata locally. Shortcut method for {@link
   * #updateMetadata(Member, Map, int)}.
   *
   * @param metadata local member metadata
   * @param incarnation
   * @return old metadata or null
   */
  Map<String, String> updateMetadata(Map<String, String> metadata, int incarnation);

  /**
   * Updates cluster member metadata locally.
   *
   * @param member member
   * @param metadata cluster member metadtaa
   * @param incarnation
   * @return old metadata or null
   */
  Map<String, String> updateMetadata(Member member, Map<String, String> metadata, int incarnation);

  /**
   * Retrives cluster member metadata remotely.
   *
   * @param member cluster member
   * @return mono result of getting remote member metadata
   */
  Mono<Map<String, String>> fetchMetadata(Member member);

  /**
   * Removes cluster member metadata from store. Attemp to remove local cluster member metadata will
   * be ignored.
   *
   * @param member cluster member
   * @return old metadata or null
   */
  Map<String, String> removeMetadata(Member member);

}
