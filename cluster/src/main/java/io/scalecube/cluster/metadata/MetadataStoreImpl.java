package io.scalecube.cluster.metadata;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.CorrelationIdGenerator;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.net.Address;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
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

  private final Map<Member, Map<String, String>> membersMetadata = new ConcurrentHashMap<>();

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
      Map<String, String> metadata,
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
  public Map<String, String> metadata() {
    return metadata(localMember);
  }

  @Override
  public Map<String, String> metadata(Member member) {
    return membersMetadata.get(member);
  }

  @Override
  public Map<String, String> updateMetadata(Map<String, String> metadata) {
    return updateMetadata(localMember, metadata);
  }

  @Override
  public Map<String, String> updateMetadata(Member member, Map<String, String> metadata) {
    Map<String, String> memberMetadata =
        Collections.unmodifiableMap(new HashMap<>(Objects.requireNonNull(metadata)));
    Map<String, String> result = membersMetadata.put(member, memberMetadata);

    if (localMember.equals(member)) {
      // added
      if (result == null) {
        LOGGER.debug("Added metadata: {} for local member {}", memberMetadata, localMember);
      } else {
        LOGGER.debug("Updated metadata: {} for local member {}", memberMetadata, localMember);
      }
    } else {
      // updated
      if (result == null) {
        LOGGER.debug(
            "Added metadata: {} for member {} [at {}]", memberMetadata, member, localMember);
      } else {
        LOGGER.debug(
            "Updated metadata: {} for member {} [at {}]", memberMetadata, member, localMember);
      }
    }
    return result;
  }

  @Override
  public Map<String, String> removeMetadata(Member member) {
    if (localMember.equals(member)) {
      throw new IllegalArgumentException("removeMetadata must not accept local member");
    }
    // remove
    Map<String, String> metadata = membersMetadata.remove(member);
    if (metadata != null) {
      LOGGER.debug("Removed metadata for member {} [at {}]", member, localMember);
      return metadata;
    }
    return null;
  }

  @Override
  public Mono<Map<String, String>> fetchMetadata(Member member) {
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
              .timeout(Duration.ofMillis(config.getMetadataTimeout()), scheduler)
              .publishOn(scheduler)
              .subscribe(
                  response -> {
                    LOGGER.debug(
                        "Received GetMetadataResp[{}] from {} [at {}]",
                        cid,
                        targetAddress,
                        localMember);
                    GetMetadataResponse respData = response.data();
                    Map<String, String> metadata = respData.getMetadata();
                    sink.success(metadata);
                  },
                  th -> {
                    LOGGER.warn(
                        "Failed getting GetMetadataResp[{}] "
                            + "from {} within {} ms [at {}], cause : {}",
                        cid,
                        targetAddress,
                        config.getMetadataTimeout(),
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
    GetMetadataResponse respData = new GetMetadataResponse(localMember, metadata());

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
