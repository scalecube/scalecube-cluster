package io.scalecube.cluster.metadata;

import io.scalecube.cluster.Member;
import java.nio.ByteBuffer;
import java.util.Optional;
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
   * Returns local cluster member metadata from local store.
   *
   * @return local member metadata
   */
  <T> Optional<T> metadata();

  /**
   * Returns cluster member metadata from local store.
   *
   * @param member cluster member
   * @return metadata of the cluster member
   */
  Optional<ByteBuffer> metadata(Member member);

  /**
   * Updates local cluster member metadata.
   *
   * @param metadata local member metadata
   */
  void updateMetadata(Object metadata);

  /**
   * Updates cluster member metadata in store.
   *
   * @param member member
   * @param metadata cluster member metadata
   * @return old metadata or null
   */
  ByteBuffer updateMetadata(Member member, ByteBuffer metadata);

  /**
   * Retrives metadata from cluster member.
   *
   * @param member cluster member
   * @return mono result of getting member metadata
   */
  Mono<ByteBuffer> fetchMetadata(Member member);

  /**
   * Removes cluster member metadata from store.
   *
   * @param member cluster member
   * @return old metadata or null
   */
  ByteBuffer removeMetadata(Member member);
}
