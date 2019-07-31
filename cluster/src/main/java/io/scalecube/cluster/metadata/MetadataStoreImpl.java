package io.scalecube.cluster.metadata;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.CorrelationIdGenerator;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.net.Address;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class MetadataStoreImpl implements MetadataStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataStoreImpl.class);

  // Qualifiers

  public static final String GET_METADATA_REQ = "sc/metadata/req";
  public static final String GET_METADATA_RESP = "sc/metadata/resp";

  // Injected

  private Object localMetadata;
  private final Member localMember;
  private final Transport transport;
  private final ClusterConfig config;
  private final CorrelationIdGenerator cidGenerator;

  // State

  private final Map<Member, ByteBuffer> membersMetadata = new ConcurrentHashMap<>();

  // Scheduler

  private final Scheduler scheduler;

  // Disposables

  private final Disposable.Composite actionsDisposables = Disposables.composite();

  /**
   * Constructor.
   *
   * @param localMember local member
   * @param transport transport
   * @param localMetadata local metadata (optional)
   * @param config config
   * @param scheduler scheduler
   * @param cidGenerator correlationId generator
   */
  public MetadataStoreImpl(
      Member localMember,
      Transport transport,
      Object localMetadata,
      ClusterConfig config,
      Scheduler scheduler,
      CorrelationIdGenerator cidGenerator) {
    this.localMember = Objects.requireNonNull(localMember);
    this.transport = Objects.requireNonNull(transport);
    this.config = Objects.requireNonNull(config);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.cidGenerator = Objects.requireNonNull(cidGenerator);
    this.localMetadata = localMetadata; // optional
  }

  @Override
  public void start() {
    // Subscribe
    actionsDisposables.add(
        // Listen to incoming get_metadata requests from other members
        transport
            .listen() //
            .publishOn(scheduler)
            .subscribe(this::onMessage, this::onError));
  }

  @Override
  public void stop() {
    // Stop accepting requests
    actionsDisposables.dispose();
    // Clear metadata hash map
    membersMetadata.clear();
  }

  @Override
  public <T> Optional<T> metadata() {
    //noinspection unchecked
    return Optional.ofNullable((T) localMetadata);
  }

  @Override
  public Optional<ByteBuffer> metadata(Member member) {
    return Optional.ofNullable(membersMetadata.get(member)).map(ByteBuffer::slice);
  }

  @Override
  public void updateMetadata(Object metadata) {
    localMetadata = metadata;
  }

  @Override
  public ByteBuffer updateMetadata(Member member, ByteBuffer metadata) {
    if (localMember.equals(member)) {
      throw new IllegalArgumentException("removeMetadata must not accept local member");
    }

    // If metadata is null then 'update' turns into 'remove'
    if (metadata == null) {
      return removeMetadata(member);
    }

    ByteBuffer value = metadata.slice();
    ByteBuffer result = membersMetadata.put(member, value);

    // updated
    if (result == null) {
      LOGGER.debug(
          "Added metadata: {} for member {} [at {}]", value.remaining(), member, localMember);
    } else {
      LOGGER.debug(
          "Updated metadata: {} for member {} [at {}]", value.remaining(), member, localMember);
    }
    return result;
  }

  @Override
  public ByteBuffer removeMetadata(Member member) {
    if (localMember.equals(member)) {
      throw new IllegalArgumentException("removeMetadata must not accept local member");
    }
    // remove
    ByteBuffer metadata = membersMetadata.remove(member);
    if (metadata != null) {
      LOGGER.debug("Removed metadata for member {} [at {}]", member, localMember);
      return metadata;
    }
    return null;
  }

  @Override
  public Mono<ByteBuffer> fetchMetadata(Member member) {
    return Mono.create(
        sink -> {
          LOGGER.debug("Getting metadata for member {} [at {}]", member, localMember);

          // Increment counter
          final String cid = cidGenerator.nextCid();
          final Address targetAddress = member.address();
          Message request =
              Message.builder()
                  .qualifier(GET_METADATA_REQ)
                  .correlationId(cid)
                  .data(new GetMetadataRequest(member))
                  .build();

          transport
              .requestResponse(targetAddress, request)
              .timeout(Duration.ofMillis(config.metadataTimeout()), scheduler)
              .publishOn(scheduler)
              .subscribe(
                  response -> {
                    LOGGER.debug(
                        "Received GetMetadataResp[{}] from {} [at {}]",
                        cid,
                        targetAddress,
                        localMember);
                    GetMetadataResponse respData = response.data();
                    ByteBuffer metadata = respData.getMetadata();
                    sink.success(metadata);
                  },
                  th -> {
                    LOGGER.warn(
                        "Failed getting GetMetadataResp[{}] "
                            + "from {} within {} ms [at {}], cause : {}",
                        cid,
                        targetAddress,
                        config.metadataTimeout(),
                        localMember,
                        th.toString());
                    sink.error(th);
                  });
        });
  }

  // ================================================
  // ============== Event Listeners =================
  // ================================================

  private void onMessage(Message message) {
    if (GET_METADATA_REQ.equals(message.qualifier())) {
      onMetadataRequest(message);
    }
  }

  private void onError(Throwable throwable) {
    LOGGER.error("Received unexpected error: ", throwable);
  }

  private void onMetadataRequest(Message message) {
    LOGGER.debug("Received GetMetadataReq: {} [at {}]", message, localMember);

    GetMetadataRequest reqData = message.data();
    Member targetMember = reqData.getMember();

    // Validate target member
    if (!targetMember.id().equals(localMember.id())) {
      LOGGER.warn(
          "Received GetMetadataReq: {} to {}, but local member is {}",
          message,
          targetMember,
          localMember);
      return;
    }

    // Prepare repopnse
    ByteBuffer byteBuffer = config.metadataEncoder().encode(localMetadata);
    GetMetadataResponse respData = new GetMetadataResponse(localMember, byteBuffer);

    Message response =
        Message.builder()
            .qualifier(GET_METADATA_RESP)
            .correlationId(message.correlationId())
            .data(respData)
            .build();

    Address responseAddress = message.sender();
    LOGGER.debug("Send GetMetadataResp: {} to {} [at {}]", response, responseAddress, localMember);
    transport
        .send(responseAddress, response)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "Failed to send GetMetadataResp: {} to {} [at {}], cause: {}",
                    response,
                    responseAddress,
                    localMember,
                    ex.toString()));
  }
}
