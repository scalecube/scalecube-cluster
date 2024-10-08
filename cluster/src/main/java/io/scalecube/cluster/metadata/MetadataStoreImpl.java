package io.scalecube.cluster.metadata;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class MetadataStoreImpl implements MetadataStore {

  private static final Logger LOGGER = System.getLogger(MetadataStore.class.getName());

  // Qualifiers

  public static final String GET_METADATA_REQ = "sc/metadata/req";
  public static final String GET_METADATA_RESP = "sc/metadata/resp";

  public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  // Injected

  private Object localMetadata;
  private final Member localMember;
  private final Transport transport;
  private final ClusterConfig config;

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
                ex -> LOGGER.log(Level.ERROR, "[{0}][onMessage][error] cause:", localMember, ex)));
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
      LOGGER.log(
          Level.DEBUG,
          "[{0}] Added metadata(size={1,number,#}) for member {2}",
          localMember,
          value.remaining(),
          member);
    } else {
      LOGGER.log(
          Level.DEBUG,
          "[{0}] Updated metadata(size={1,number,#}) for member {2}",
          localMember,
          value.remaining(),
          member);
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
      LOGGER.log(
          Level.DEBUG,
          "[{0}] Removed metadata(size={1,number,#}) for member {2}",
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
          final String targetAddress = member.address();

          LOGGER.log(
              Level.DEBUG, "[{0}][{1}] Getting metadata for member {2}", localMember, cid, member);

          Message request =
              Message.builder()
                  .qualifier(GET_METADATA_REQ)
                  .correlationId(cid)
                  .data(new GetMetadataRequest(member))
                  .build();

          return transport
              .requestResponse(targetAddress, request)
              .timeout(Duration.ofMillis(config.metadataTimeout()), scheduler)
              .publishOn(scheduler)
              .doOnSuccess(
                  s ->
                      LOGGER.log(
                          Level.DEBUG,
                          "[{0}][{1}] Received GetMetadataResp from {2}",
                          localMember,
                          cid,
                          targetAddress))
              .map(Message::<GetMetadataResponse>data)
              .map(GetMetadataResponse::getMetadata)
              .doOnError(
                  th ->
                      LOGGER.log(
                          Level.WARNING,
                          "[{0}][{1}] Timeout getting GetMetadataResp "
                              + "from {2} within {3,number,#}ms, cause: {4}",
                          localMember,
                          cid,
                          targetAddress,
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
    final String sender = message.sender();
    LOGGER.log(Level.DEBUG, "[{0}] Received GetMetadataReq from {1}", localMember, sender);

    GetMetadataRequest reqData = message.data();
    Member targetMember = reqData.getMember();

    // Validate target member
    if (!targetMember.id().equals(localMember.id())) {
      LOGGER.log(
          Level.WARNING,
          "[{0}] Received GetMetadataReq from {1} to {2}, but local member is {3}",
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

    LOGGER.log(Level.DEBUG, "[{0}] Send GetMetadataResp to {1}", localMember, sender);
    transport
        .send(sender, response)
        .subscribe(
            null,
            ex ->
                LOGGER.log(
                    Level.DEBUG,
                    "[{0}] Failed to send GetMetadataResp to {1}, cause: {2}",
                    localMember,
                    sender,
                    ex.toString()));
  }

  private ByteBuffer encodeMetadata() {
    ByteBuffer result = null;
    try {
      result = config.metadataCodec().serialize(localMetadata);
    } catch (Exception e) {
      LOGGER.log(
          Level.ERROR,
          "[{0}] Failed to encode metadata: {1}, cause: {2}",
          localMember,
          localMetadata,
          e.toString());
    }
    return Optional.ofNullable(result).orElse(EMPTY_BUFFER);
  }
}
