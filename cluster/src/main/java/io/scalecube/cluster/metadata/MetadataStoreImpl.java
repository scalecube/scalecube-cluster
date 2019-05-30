package io.scalecube.cluster.metadata;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.CorrelationIdGenerator;
import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
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
   * @param metadata local metadata
   * @param config config
   * @param scheduler scheduler
   * @param cidGenerator correlationId generator
   */
  public MetadataStoreImpl(
      Member localMember,
      Transport transport,
      ByteBuffer metadata,
      ClusterConfig config,
      Scheduler scheduler,
      CorrelationIdGenerator cidGenerator) {
    this.localMember = Objects.requireNonNull(localMember);
    this.transport = Objects.requireNonNull(transport);
    this.config = Objects.requireNonNull(config);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.cidGenerator = Objects.requireNonNull(cidGenerator);
    // store local metadata
    updateMetadata(Objects.requireNonNull(metadata));
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
  public ByteBuffer metadata() {
    return metadata(localMember);
  }

  @Override
  public ByteBuffer metadata(Member member) {
    return membersMetadata.get(member);
  }

  @Override
  public ByteBuffer updateMetadata(ByteBuffer metadata) {
    return updateMetadata(localMember, metadata);
  }

  @Override
  public ByteBuffer updateMetadata(Member member, ByteBuffer metadata) {
    ByteBuffer result = membersMetadata.put(member, Objects.requireNonNull(metadata));

    if (localMember.equals(member)) {
      // added
      if (result == null) {
        LOGGER.debug("Added metadata: {} for local member {}", metadata.capacity(), localMember);
      } else {
        LOGGER.debug("Updated metadata: {} for local member {}", metadata.capacity(), localMember);
      }
    } else {
      // updated
      if (result == null) {
        LOGGER.debug("Added metadata: {} for member {}", metadata.capacity(), member);
      } else {
        LOGGER.debug("Updated metadata: {} for member {}", metadata.capacity(), member);
      }
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
      LOGGER.debug("Removed metadata for member {}", member);
      return metadata;
    }
    return null;
  }

  @Override
  public Mono<ByteBuffer> fetchMetadata(Member member) {
    return Mono.create(
        sink -> {
          LOGGER.debug("Getting metadata for member {}", member);

          // Increment counter
          final String cid = cidGenerator.nextCid();
          final Address targetAddress = member.address();
          Message request =
              Message.builder()
                  .qualifier(GET_METADATA_REQ)
                  .correlationId(cid)
                  .data(new GetMetadataRequest(member))
                  .sender(localMember.address())
                  .build();

          transport
              .requestResponse(request, targetAddress)
              .timeout(Duration.ofMillis(config.getMetadataTimeout()), scheduler)
              .publishOn(scheduler)
              .subscribe(
                  response -> {
                    LOGGER.debug("Received GetMetadataResp[{}] from {}", cid, targetAddress);
                    GetMetadataResponse respData = response.data();
                    ByteBuffer metadata = respData.getMetadata();
                    sink.success(metadata);
                  },
                  th -> {
                    LOGGER.warn(
                        "Failed getting GetMetadataResp[{}] from {} within {} ms, cause : {}",
                        cid,
                        targetAddress,
                        config.getMetadataTimeout(),
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
    LOGGER.debug("Received GetMetadataReq: {}", message);

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
    GetMetadataResponse respData = new GetMetadataResponse(localMember, metadata());

    Message response =
        Message.builder()
            .qualifier(GET_METADATA_RESP)
            .correlationId(message.correlationId())
            .sender(localMember.address())
            .data(respData)
            .build();

    Address responseAddress = message.sender();
    LOGGER.debug("Send GetMetadataResp: {} to {}", response, responseAddress);
    transport
        .send(responseAddress, response)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "Failed to send GetMetadataResp: {} to {}, cause: {}",
                    response,
                    responseAddress,
                    ex.toString()));
  }
}
