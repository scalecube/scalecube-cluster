package io.scalecube.cluster.metadata;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Transport;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.MonoProcessor;

public class MetadataStoreImpl implements MetadataStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataStoreImpl.class);

  private final Member localMember;
  private final Transport transport;

  private final Map<Member, MetadataPromise> membersMetadata = new HashMap<>();

  public MetadataStoreImpl(
      Member localMember, Transport transport, Map<String, String> localMetadata) {
    this.localMember = Objects.requireNonNull(localMember);
    this.transport = Objects.requireNonNull(transport);
    membersMetadata.put(localMember, new MetadataPromise(localMetadata));
  }

  @Override
  public Map<String, String> metadata() {
    MetadataPromise promise = membersMetadata.get(localMember);
    if (promise == null) {
      throw new IllegalStateException();
    }
    return promise.metadata();
  }

  @Override
  public void updateMetadata(Map<String, String> metadata) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Update local member {} with metadata: {}", localMember, metadata);
    }
    membersMetadata.
    localMetadata.clear();
    localMetadata.putAll(new HashMap<>(Objects.requireNonNull(metadata)));
  }

  public void markMetadataForUpdate(Member member) {}

  public void removeMetadata(Member member) {}

  static class MetadataPromise {

    private final MonoProcessor<MetadataResponse> promise;

    MetadataPromise() {
      promise = MonoProcessor.create();
    }

    MetadataPromise(Map<String, String> metadata) {
      promise = MonoProcessor.create();
      promise.onNext(new MetadataResponse(metadata));
      promise.onComplete();
    }

    Map<String, String> metadata() {
      if (!promise.isSuccess()) {
        throw new IllegalStateException();
      }
      return new HashMap<>(promise.block().metadata);
    }
  }

  static class MetadataResponse {

    private final Map<String, String> metadata;

    MetadataResponse(Map<String, String> metadata) {
      this.metadata = new HashMap<>(Objects.requireNonNull(metadata));
    }
  }
}
