package io.scalecube.cluster.fdetector;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MemberStatus;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

public final class FailureDetectorImpl implements FailureDetector {

  private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetectorImpl.class);

  // Qualifiers

  public static final String PING = "sc/fdetector/ping";
  public static final String PING_REQ = "sc/fdetector/pingReq";
  public static final String PING_ACK = "sc/fdetector/pingAck";

  // Injected

  private final Member localMember;
  private final Transport transport;
  private final FailureDetectorConfig config;

  // State

  private long period = 0;
  private List<Member> pingMembers = new ArrayList<>();
  private int pingMemberIndex = 0; // index for sequential ping member selection

  // Disposables

  private final Disposable.Composite actionsDisposables = Disposables.composite();

  // Subject
  private final FluxProcessor<FailureDetectorEvent, FailureDetectorEvent> subject =
      DirectProcessor.<FailureDetectorEvent>create().serialize();

  private final FluxSink<FailureDetectorEvent> sink = subject.sink();

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
            membershipProcessor //
                .publishOn(scheduler)
                .subscribe(this::onMemberEvent, this::onError),
            transport
                .listen() //
                .publishOn(scheduler)
                .subscribe(this::onMessage, this::onError)));
  }

  @Override
  public void start() {
    actionsDisposables.add(
        scheduler.schedulePeriodically(
            this::doPing,
            config.getPingInterval(),
            config.getPingInterval(),
            TimeUnit.MILLISECONDS));
  }

  @Override
  public void stop() {
    // Stop accepting requests and sending pings
    actionsDisposables.dispose();

    // Stop publishing events
    sink.complete();
  }

  @Override
  public Flux<FailureDetectorEvent> listen() {
    return subject.onBackpressureBuffer();
  }

  // ================================================
  // ============== Action Methods ==================
  // ================================================

  private void doPing() {
    // Increment period counter
    period++;

    // Select ping member
    Member pingMember = selectPingMember();
    if (pingMember == null) {
      return;
    }

    // Send ping
    String cid = localMember.id() + "-" + period;
    PingData pingData = new PingData(localMember, pingMember);
    Message pingMsg =
        Message.withData(pingData)
            .qualifier(PING)
            .correlationId(cid)
            .sender(localMember.address())
            .build();

    transport
        .listen()
        .filter(this::isPingAck)
        .filter(message -> cid.equals(message.correlationId()))
        .take(1)
        .timeout(Duration.ofMillis(config.getPingTimeout()), scheduler)
        .publishOn(scheduler)
        .subscribe(
            message -> {
              LOGGER.trace("Received PingAck[{}] from {}", period, pingMember);
              publishPingResult(pingMember, MemberStatus.ALIVE);
            },
            throwable -> {
              LOGGER.trace(
                  "Timeout getting PingAck[{}] from {} within {} ms",
                  period,
                  pingMember,
                  config.getPingTimeout());
              doPingReq(pingMember, cid);
            });

    LOGGER.trace("Send Ping[{}] to {}", period, pingMember);
    Address address = pingMember.address();
    transport
        .send(address, pingMsg)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "Failed to send Ping[{}] to {}, cause: {}", period, address, ex.toString()));
  }

  private void doPingReq(final Member pingMember, String cid) {
    final int timeout = config.getPingInterval() - config.getPingTimeout();
    if (timeout <= 0) {
      LOGGER.trace(
          "No PingReq[{}] occurred, because no time left (pingInterval={}, pingTimeout={})",
          period,
          config.getPingInterval(),
          config.getPingTimeout());
      publishPingResult(pingMember, MemberStatus.SUSPECT);
      return;
    }

    final List<Member> pingReqMembers = selectPingReqMembers(pingMember);
    if (pingReqMembers.isEmpty()) {
      LOGGER.trace("No PingReq[{}] occurred, because member selection is empty", period);
      publishPingResult(pingMember, MemberStatus.SUSPECT);
      return;
    }

    transport
        .listen()
        .filter(this::isPingAck)
        .filter(message -> cid.equals(message.correlationId()))
        .take(1)
        .timeout(Duration.ofMillis(timeout), scheduler)
        .publishOn(scheduler)
        .subscribe(
            message -> {
              LOGGER.trace(
                  "Received transit PingAck[{}] from {} to {}",
                  period,
                  message.sender(),
                  pingMember);
              publishPingResult(pingMember, MemberStatus.ALIVE);
            },
            throwable -> {
              LOGGER.trace(
                  "Timeout getting transit PingAck[{}] from {} to {} within {} ms",
                  period,
                  pingReqMembers,
                  pingMember,
                  timeout);
              publishPingResult(pingMember, MemberStatus.SUSPECT);
            });

    PingData pingReqData = new PingData(localMember, pingMember);
    Message pingReqMsg =
        Message.withData(pingReqData)
            .qualifier(PING_REQ)
            .correlationId(cid)
            .sender(localMember.address())
            .build();
    LOGGER.trace("Send PingReq[{}] to {} for {}", period, pingReqMembers, pingMember);

    Flux.fromIterable(pingReqMembers)
        .flatMap(member -> transport.send(member.address(), pingReqMsg))
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "Failed to send PingReq[{}] for {}, cause: {}",
                    period,
                    pingMember,
                    ex.toString()));
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
    LOGGER.trace("Received Ping[{}]", period);
    PingData data = message.data();
    if (!data.getTo().id().equals(localMember.id())) {
      LOGGER.warn(
          "Received Ping[{}] to {}, but local member is {}", period, data.getTo(), localMember);
      return;
    }
    String correlationId = message.correlationId();
    Message ackMessage =
        Message.withData(data)
            .qualifier(PING_ACK)
            .correlationId(correlationId)
            .sender(localMember.address())
            .build();
    Address address = data.getFrom().address();
    LOGGER.trace("Send PingAck[{}] to {}", period, address);
    transport
        .send(address, ackMessage)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "Failed to send PingAck[{}] to {}, cause: {}", period, address, ex.toString()));
  }

  /** Listens to PING_REQ message and sends PING to requested cluster member. */
  private void onPingReq(Message message) {
    LOGGER.trace("Received PingReq[{}]", period);
    PingData data = message.data();
    Member target = data.getTo();
    Member originalIssuer = data.getFrom();
    String correlationId = message.correlationId();
    PingData pingReqData = new PingData(localMember, target, originalIssuer);
    Message pingMessage =
        Message.withData(pingReqData)
            .qualifier(PING)
            .correlationId(correlationId)
            .sender(localMember.address())
            .build();
    Address address = target.address();
    LOGGER.trace("Send transit Ping[{}] to {}", period, address);
    transport
        .send(address, pingMessage)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "Failed to send transit Ping[{}] to {}, cause: {}",
                    period,
                    address,
                    ex.toString()));
  }

  /**
   * Listens to ACK with message containing ORIGINAL_ISSUER then converts message to plain ACK and
   * sends it to ORIGINAL_ISSUER.
   */
  private void onTransitPingAck(Message message) {
    LOGGER.trace("Received transit PingAck[{}]", period);
    PingData data = message.data();
    Member target = data.getOriginalIssuer();
    String correlationId = message.correlationId();
    PingData originalAckData = new PingData(target, data.getTo());
    Message originalAckMessage =
        Message.withData(originalAckData)
            .qualifier(PING_ACK)
            .correlationId(correlationId)
            .sender(localMember.address())
            .build();
    Address address = target.address();
    LOGGER.trace("Resend transit PingAck[{}] to {}", period, address);
    transport
        .send(address, originalAckMessage)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "Failed to resend transit PingAck[{}] to {}, cause: {}",
                    period,
                    address,
                    ex.toString()));
  }

  private void onError(Throwable throwable) {
    LOGGER.error("Received unexpected error[{}]: ", period, throwable);
  }

  private void onMemberEvent(MembershipEvent event) {
    Member member = event.member();
    if (event.isRemoved()) {
      pingMembers.remove(member);
    }
    if (event.isAdded()) {
      // insert member into random positions
      int size = pingMembers.size();
      int index = size > 0 ? ThreadLocalRandom.current().nextInt(size) : 0;
      pingMembers.add(index, member);
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
    if (config.getPingReqMembers() <= 0) {
      return Collections.emptyList();
    }
    List<Member> candidates = new ArrayList<>(pingMembers);
    candidates.remove(pingMember);
    if (candidates.isEmpty()) {
      return Collections.emptyList();
    }
    Collections.shuffle(candidates);
    boolean selectAll = candidates.size() < config.getPingReqMembers();
    return selectAll ? candidates : candidates.subList(0, config.getPingReqMembers());
  }

  private void publishPingResult(Member member, MemberStatus status) {
    LOGGER.debug("Member {} detected as {} at [{}]", member, status, period);
    sink.next(new FailureDetectorEvent(member, status));
  }

  private boolean isPing(Message message) {
    return PING.equals(message.qualifier());
  }

  private boolean isPingReq(Message message) {
    return PING_REQ.equals(message.qualifier());
  }

  private boolean isPingAck(Message message) {
    return PING_ACK.equals(message.qualifier())
        && message.<PingData>data().getOriginalIssuer() == null;
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
