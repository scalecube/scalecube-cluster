package io.scalecube.cluster.metadata;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.TransportWrapper;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class MetadataStoreImpl implements MetadataStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataStore.class);

  // Qualifiers

  public static final String GET_METADATA_REQ = "sc/metadata/req";
  public static final String GET_METADATA_RESP = "sc/metadata/resp";

  public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  // Injected

  private Object localMetadata;
  private final Member localMember;
  private final Transport transport;
  private final ClusterConfig config;
  private final TransportWrapper transportWrapper;

  // State

  private final Map<Member, ByteBuffer> membersMetadata = new HashMap<>();

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
   */
  public MetadataStoreImpl(
      Member localMember,
      Transport transport,
      Object localMetadata,
      ClusterConfig config,
      Scheduler scheduler) {
    this.localMember = Objects.requireNonNull(localMember);
    this.transport = Objects.requireNonNull(transport);
    this.config = Objects.requireNonNull(config);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.localMetadata = localMetadata; // optional
    transportWrapper = new TransportWrapper(transport);
  }

  @Override
  public void start() {
    // Subscribe
    actionsDisposables.add(
        // Listen to incoming get_metadata requests from other members
        transport
            .listen()
            .publishOn(scheduler)
            .subscribe(
                this::onMessage,
                ex -> LOGGER.error("[{}][onMessage][error] cause:", localMember, ex)));
  }

  @Override
  public void stop() {
    actionsDisposables.dispose();
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

    ByteBuffer value = metadata.slice();
    ByteBuffer result = membersMetadata.put(member, value);

    if (result == null) {
      LOGGER.debug(
          "[{}] Added metadata(size={}) for member {}", localMember, value.remaining(), member);
    } else {
      LOGGER.debug(
          "[{}] Updated metadata(size={}) for member {}", localMember, value.remaining(), member);
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
      LOGGER.debug(
          "[{}] Removed metadata(size={}) for member {}",
          localMember,
          metadata.remaining(),
          member);
      return metadata;
    }
    return null;
  }

  @Override
  public Mono<ByteBuffer> fetchMetadata(Member member) {
    return Mono.defer(
        () -> {
          final String cid = UUID.randomUUID().toString();

          LOGGER.debug("[{}][{}] Getting metadata for member {}", localMember, cid, member);

          Message request =
              Message.builder()
                  .qualifier(GET_METADATA_REQ)
                  .correlationId(cid)
                  .data(new GetMetadataRequest(member))
                  .build();

          return transportWrapper
              .requestResponse(member, request)
              .timeout(Duration.ofMillis(config.metadataTimeout()), scheduler)
              .publishOn(scheduler)
              .doOnSuccess(
                  s ->
                      LOGGER.debug(
                          "[{}][{}] Received GetMetadataResp from {}",
                          localMember,
                          cid,
                          member.addresses()))
              .map(Message::<GetMetadataResponse>data)
              .map(GetMetadataResponse::getMetadata)
              .doOnError(
                  th ->
                      LOGGER.warn(
                          "[{}][{}] Timeout getting GetMetadataResp "
                              + "from {} within {} ms, cause: {}",
                          localMember,
                          cid,
                          member.addresses(),
                          config.metadataTimeout(),
                          th.toString()));
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

  private void onMetadataRequest(Message message) {
    final Member sender = (Member) message.sender();
    LOGGER.debug("[{}] Received GetMetadataReq from {}", localMember, sender);

    GetMetadataRequest reqData = message.data();
    Member targetMember = reqData.getMember();

    // Validate target member
    if (!targetMember.id().equals(localMember.id())) {
      LOGGER.warn(
          "[{}] Received GetMetadataReq from {} to {}, but local member is {}",
          localMember,
          sender,
          targetMember,
          localMember);
      return;
    }

    // Prepare response
    GetMetadataResponse respData = new GetMetadataResponse(localMember, encodeMetadata());

    Message response =
        Message.builder()
            .qualifier(GET_METADATA_RESP)
            .correlationId(message.correlationId())
            .data(respData)
            .build();

    LOGGER.debug("[{}] Send GetMetadataResp to {}", localMember, sender);

    transportWrapper
        .send(sender, response)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "[{}] Failed to send GetMetadataResp to {}, cause: {}",
                    localMember,
                    sender,
                    ex.toString()));
  }

  private ByteBuffer encodeMetadata() {
    ByteBuffer result = null;
    try {
      result = config.metadataCodec().serialize(localMetadata);
    } catch (Exception e) {
      LOGGER.error(
          "[{}] Failed to encode metadata: {}, cause: {}",
          localMember,
          localMetadata,
          e.toString());
    }
    return Optional.ofNullable(result).orElse(EMPTY_BUFFER);
  }
}
