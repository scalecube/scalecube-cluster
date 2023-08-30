package io.scalecube.cluster.fdetector;

import static io.scalecube.reactor.RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.fdetector.PingData.AckType;
import io.scalecube.cluster.membership.MemberStatus;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportWrapper;
import io.scalecube.net.Address;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

public final class FailureDetectorImpl implements FailureDetector {

  private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetector.class);

  // Qualifiers

  public static final String PING = "sc/fdetector/ping";
  public static final String PING_REQ = "sc/fdetector/pingReq";
  public static final String PING_ACK = "sc/fdetector/pingAck";

  // Injected

  private final Member localMember;
  private final Transport transport;
  private final FailureDetectorConfig config;

  // State

  private final List<Member> pingMembers = new ArrayList<>();
  private long currentPeriod = 0;
  private int pingMemberIndex = 0; // index for sequential ping member selection

  // Disposables

  private final Disposable.Composite actionsDisposables = Disposables.composite();

  // Subject

  private final Sinks.Many<FailureDetectorEvent> sink = Sinks.many().multicast().directBestEffort();

  // Scheduled

  private final Scheduler scheduler;

  /**
   * Creates new instance of failure detector with given transport and settings.
   *
   * @param localMember local cluster member
   * @param transport cluster transport
   * @param membershipProcessor membership event processor
   * @param config failure detector settings
   * @param scheduler scheduler
   */
  public FailureDetectorImpl(
      Member localMember,
      Transport transport,
      Flux<MembershipEvent> membershipProcessor,
      FailureDetectorConfig config,
      Scheduler scheduler) {

    this.localMember = Objects.requireNonNull(localMember);
    this.transport = Objects.requireNonNull(transport);
    this.config = Objects.requireNonNull(config);
    this.scheduler = Objects.requireNonNull(scheduler);

    // Subscribe
    actionsDisposables.addAll(
        Arrays.asList(
            transport
                .listen() // Listen failure detector requests
                .publishOn(scheduler)
                .subscribe(
                    this::onMessage,
                    ex ->
                        LOGGER.error(
                            "[{}][{}][onMessage][error] cause:", localMember, currentPeriod, ex)),
            membershipProcessor // Listen membership events to update remoteMembers
                .publishOn(scheduler)
                .subscribe(
                    this::onMembershipEvent,
                    ex ->
                        LOGGER.error(
                            "[{}][{}][onMembershipEvent][error] cause:",
                            localMember,
                            currentPeriod,
                            ex))));
  }

  @Override
  public void start() {
    actionsDisposables.add(
        scheduler.schedulePeriodically(
            this::doPing, config.pingInterval(), config.pingInterval(), TimeUnit.MILLISECONDS));
  }

  @Override
  public void stop() {
    // Stop accepting requests and sending pings
    actionsDisposables.dispose();

    // Stop publishing events
    sink.emitComplete(RETRY_NON_SERIALIZED);
  }

  @Override
  public Flux<FailureDetectorEvent> listen() {
    return sink.asFlux().onBackpressureBuffer();
  }

  // ================================================
  // ============== Action Methods ==================
  // ================================================

  private void doPing() {
    // Increment period counter
    long period = currentPeriod++;

    // Select ping member
    Member pingMember = selectPingMember();
    if (pingMember == null) {
      return;
    }

    // Send ping
    String cid = UUID.randomUUID().toString();
    PingData pingData = new PingData(localMember, pingMember);
    Message pingMsg = Message.withData(pingData).qualifier(PING).correlationId(cid).build();

    LOGGER.debug("[{}][{}] Send Ping to {}", localMember, period, pingMember);
    List<Address> addresses = pingMember.addresses();
    TransportWrapper.requestResponse(transport, addresses, pingMsg)
        .timeout(Duration.ofMillis(config.pingTimeout()), scheduler)
        .publishOn(scheduler)
        .subscribe(
            message -> {
              LOGGER.debug(
                  "[{}][{}] Received PingAck from {}", localMember, period, message.sender());
              publishPingResult(period, pingMember, computeMemberStatus(message, period));
            },
            ex -> {
              LOGGER.debug(
                  "[{}][{}] Failed to get PingAck from {} within {} ms",
                  localMember,
                  period,
                  pingMember,
                  config.pingTimeout());

              final int timeLeft = config.pingInterval() - config.pingTimeout();
              final List<Member> pingReqMembers = selectPingReqMembers(pingMember);

              if (timeLeft <= 0 || pingReqMembers.isEmpty()) {
                LOGGER.debug("[{}][{}] No PingReq occurred", localMember, period);
                publishPingResult(period, pingMember, MemberStatus.SUSPECT);
              } else {
                doPingReq(currentPeriod, pingMember, pingReqMembers, cid);
              }
            });
  }

  private void doPingReq(
      long period, final Member pingMember, final List<Member> pingReqMembers, String cid) {
    Message pingReqMsg =
        Message.withData(new PingData(localMember, pingMember))
            .qualifier(PING_REQ)
            .correlationId(cid)
            .build();
    LOGGER.debug(
        "[{}][{}] Send PingReq to {} for {}", localMember, period, pingReqMembers, pingMember);

    Duration timeout = Duration.ofMillis(config.pingInterval() - config.pingTimeout());
    pingReqMembers.forEach(
        member ->
            TransportWrapper.requestResponse(transport, member.addresses(), pingReqMsg)
                .timeout(timeout, scheduler)
                .publishOn(scheduler)
                .subscribe(
                    message -> {
                      LOGGER.debug(
                          "[{}][{}] Received transit PingAck from {} to {}",
                          localMember,
                          period,
                          message.sender(),
                          pingMember);
                      publishPingResult(period, pingMember, computeMemberStatus(message, period));
                    },
                    throwable -> {
                      LOGGER.debug(
                          "[{}][{}] Timeout getting transit PingAck from {} to {} within {} ms",
                          localMember,
                          period,
                          member,
                          pingMember,
                          timeout.toMillis());
                      publishPingResult(period, pingMember, MemberStatus.SUSPECT);
                    }));
  }

  // ================================================
  // ============== Event Listeners =================
  // ================================================

  private void onMessage(Message message) {
    if (isPing(message)) {
      onPing(message);
    } else if (isPingReq(message)) {
      onPingReq(message);
    } else if (isTransitPingAck(message)) {
      onTransitPingAck(message);
    }
  }

  /** Listens to PING message and answers with ACK. */
  private void onPing(Message message) {
    long period = this.currentPeriod;
    List<Address> sender = message.sender();
    LOGGER.debug("[{}][{}] Received Ping from {}", localMember, period, sender);
    PingData data = message.data();
    data = data.withAckType(AckType.DEST_OK);
    if (!data.getTo().id().equals(localMember.id())) {
      LOGGER.debug(
          "[{}][{}] Received Ping from {} to {}, but local member is {}",
          localMember,
          period,
          sender,
          data.getTo(),
          localMember);
      data = data.withAckType(AckType.DEST_GONE);
    }
    String correlationId = message.correlationId();
    Message ackMessage =
        Message.withData(data).qualifier(PING_ACK).correlationId(correlationId).build();
    List<Address> addresses = data.getFrom().addresses();
    LOGGER.debug("[{}][{}] Send PingAck to {}", localMember, period, addresses);
    TransportWrapper.send(transport, addresses, ackMessage)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "[{}][{}] Failed to send PingAck to {}, cause: {}",
                    localMember,
                    period,
                    addresses,
                    ex.toString()));
  }

  /** Listens to PING_REQ message and sends PING to requested cluster member. */
  private void onPingReq(Message message) {
    long period = this.currentPeriod;
    LOGGER.debug("[{}][{}] Received PingReq from {}", localMember, period, message.sender());
    PingData data = message.data();
    Member target = data.getTo();
    Member originalIssuer = data.getFrom();
    String correlationId = message.correlationId();
    PingData pingReqData = new PingData(localMember, target, originalIssuer);
    Message pingMessage =
        Message.withData(pingReqData).qualifier(PING).correlationId(correlationId).build();
    List<Address> addresses = target.addresses();
    LOGGER.debug("[{}][{}] Send transit Ping to {}", localMember, period, addresses);
    TransportWrapper.send(transport, addresses, pingMessage)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "[{}][{}] Failed to send transit Ping to {}, cause: {}",
                    localMember,
                    period,
                    addresses,
                    ex.toString()));
  }

  /**
   * Listens to ACK with message containing ORIGINAL_ISSUER then converts message to plain ACK and
   * sends it to ORIGINAL_ISSUER.
   */
  private void onTransitPingAck(Message message) {
    long period = this.currentPeriod;
    LOGGER.debug(
        "[{}][{}] Received transit PingAck from {}", localMember, period, message.sender());
    PingData data = message.data();
    AckType ackType = data.getAckType();
    Member target = data.getOriginalIssuer();
    String correlationId = message.correlationId();
    PingData originalAckData = new PingData(target, data.getTo()).withAckType(ackType);
    Message originalAckMessage =
        Message.withData(originalAckData).qualifier(PING_ACK).correlationId(correlationId).build();
    List<Address> addresses = target.addresses();
    LOGGER.debug("[{}][{}] Resend transit PingAck to {}", localMember, period, addresses);
    TransportWrapper.send(transport, addresses, originalAckMessage)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "[{}][{}] Failed to resend transit PingAck to {}, cause: {}",
                    localMember,
                    period,
                    addresses,
                    ex.toString()));
  }

  private void onMembershipEvent(MembershipEvent event) {
    Member member = event.member();
    if (event.isRemoved()) {
      boolean removed = pingMembers.remove(member);
      if (removed) {
        LOGGER.debug(
            "[{}][{}] Removed {} from pingMembers list (size={})",
            localMember,
            currentPeriod,
            member,
            pingMembers.size());
      }
    }
    if (event.isAdded()) {
      // insert member into random positions
      int size = pingMembers.size();
      int index = size > 0 ? ThreadLocalRandom.current().nextInt(size) : 0;
      pingMembers.add(index, member);
      LOGGER.debug(
          "[{}][{}] Added {} to pingMembers list (size={})",
          localMember,
          currentPeriod,
          member,
          pingMembers.size());
    }
  }

  // ================================================
  // ============== Helper Methods ==================
  // ================================================

  private Member selectPingMember() {
    if (pingMembers.isEmpty()) {
      return null;
    }
    if (pingMemberIndex >= pingMembers.size()) {
      pingMemberIndex = 0;
      Collections.shuffle(pingMembers);
    }
    return pingMembers.get(pingMemberIndex++);
  }

  private List<Member> selectPingReqMembers(Member pingMember) {
    if (config.pingReqMembers() <= 0) {
      return Collections.emptyList();
    }
    List<Member> candidates = new ArrayList<>(pingMembers);
    candidates.remove(pingMember);
    if (candidates.isEmpty()) {
      return Collections.emptyList();
    }
    Collections.shuffle(candidates);
    boolean selectAll = candidates.size() < config.pingReqMembers();
    return selectAll ? candidates : candidates.subList(0, config.pingReqMembers());
  }

  private void publishPingResult(long period, Member member, MemberStatus status) {
    LOGGER.debug("[{}][{}] Member {} detected as {}", localMember, period, member, status);
    sink.emitNext(new FailureDetectorEvent(member, status), RETRY_NON_SERIALIZED);
  }

  private MemberStatus computeMemberStatus(Message message, long period) {
    MemberStatus memberStatus;
    PingData data = message.data();
    AckType ackType = data.getAckType();

    if (ackType == null) {
      // support of older cluster versions
      return MemberStatus.ALIVE;
    }
    switch (ackType) {
      case DEST_OK:
        memberStatus = MemberStatus.ALIVE;
        break;
      case DEST_GONE:
        memberStatus = MemberStatus.DEAD;
        break;
      default:
        LOGGER.warn(
            "[{}][{}] Unknown PingData.AckType received '{}'", localMember, period, ackType);
        memberStatus = MemberStatus.SUSPECT;
    }
    return memberStatus;
  }

  private boolean isPing(Message message) {
    return PING.equals(message.qualifier());
  }

  private boolean isPingReq(Message message) {
    return PING_REQ.equals(message.qualifier());
  }

  private boolean isTransitPingAck(Message message) {
    return PING_ACK.equals(message.qualifier())
        && message.<PingData>data().getOriginalIssuer() != null;
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   *
   * @return transport
   */
  Transport getTransport() {
    return transport;
  }
}
