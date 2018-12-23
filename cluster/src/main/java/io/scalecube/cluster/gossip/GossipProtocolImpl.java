package io.scalecube.cluster.gossip;

import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;

public final class GossipProtocolImpl implements GossipProtocol {

  private static final Logger LOGGER = LoggerFactory.getLogger(GossipProtocolImpl.class);

  // Qualifiers

  public static final String GOSSIP_REQ = "sc/gossip/req";

  // Injected

  private final Member localMember;
  private final Transport transport;
  private final GossipConfig config;

  // Local State

  private long period = 0;
  private long gossipCounter = 0;
  private Map<String, GossipState> gossips = new HashMap<>();
  private Map<String, MonoSink<String>> futures = new HashMap<>();

  private List<Member> remoteMembers = new ArrayList<>();
  private int remoteMembersIndex = -1;

  // Disposables

  private final Disposable.Composite actionsDisposables = Disposables.composite();

  // Subject

  private final FluxProcessor<Message, Message> subject =
      DirectProcessor.<Message>create().serialize();

  private final FluxSink<Message> sink = subject.sink();

  // Scheduled

  private final Scheduler scheduler;

  /**
   * Creates new instance of gossip protocol with given memberId, transport and settings.
   *
   * @param localMember local cluster member
   * @param transport cluster transport
   * @param membershipProcessor membership event processor
   * @param config gossip protocol settings
   * @param scheduler scheduler
   */
  public GossipProtocolImpl(
      Member localMember,
      Transport transport,
      Flux<MembershipEvent> membershipProcessor,
      GossipConfig config,
      Scheduler scheduler) {

    this.transport = Objects.requireNonNull(transport);
    this.config = Objects.requireNonNull(config);
    this.localMember = Objects.requireNonNull(localMember);
    this.scheduler = Objects.requireNonNull(scheduler);

    // Subscribe
    actionsDisposables.addAll(
        Arrays.asList(
            membershipProcessor //
                .publishOn(scheduler)
                .subscribe(this::onMemberEvent, this::onError),
            transport
                .listen()
                .publishOn(scheduler)
                .filter(this::isGossipReq)
                .subscribe(this::onGossipReq, this::onError)));
  }

  @Override
  public void start() {
    actionsDisposables.add(
        scheduler.schedulePeriodically(
            this::doSpreadGossip,
            config.getGossipInterval(),
            config.getGossipInterval(),
            TimeUnit.MILLISECONDS));
  }

  @Override
  public void stop() {
    // Stop accepting gossip requests and spreading gossips
    actionsDisposables.dispose();

    // Stop publishing events
    sink.complete();
  }

  @Override
  public Mono<String> spread(Message message) {
    return Mono.fromCallable(() -> message)
        .subscribeOn(scheduler)
        .flatMap(msg -> Mono.create(sink -> futures.put(createAndPutGossip(msg), sink)));
  }

  @Override
  public Flux<Message> listen() {
    return subject.onBackpressureBuffer();
  }

  // ================================================
  // ============== Action Methods ==================
  // ================================================

  private void doSpreadGossip() {
    // Increment period
    period++;

    // Check any gossips exists
    if (gossips.isEmpty()) {
      return; // nothing to spread
    }

    try {
      // Spread gossips to randomly selected member(s)
      selectGossipMembers().forEach(this::spreadGossipsTo);

      // Sweep gossips
      sweepGossips();
    } catch (Exception ex) {
      LOGGER.warn("Exception at doSpreadGossip[{}]: {}", period, ex.getMessage(), ex);
    }
  }

  // ================================================
  // ============== Event Listeners =================
  // ================================================

  private String createAndPutGossip(Message message) {
    Gossip gossip = new Gossip(generateGossipId(), message);
    GossipState gossipState = new GossipState(gossip, period);
    gossips.put(gossip.gossipId(), gossipState);
    return gossip.gossipId();
  }

  private void onGossipReq(Message message) {
    GossipRequest gossipRequest = message.data();
    for (Gossip gossip : gossipRequest.gossips()) {
      GossipState gossipState = gossips.get(gossip.gossipId());
      if (gossipState == null) { // new gossip
        gossipState = new GossipState(gossip, period);
        gossips.put(gossip.gossipId(), gossipState);
        sink.next(gossip.message());
      }
      gossipState.addToInfected(gossipRequest.from());
    }
  }

  private void onMemberEvent(MembershipEvent event) {
    Member member = event.member();
    if (event.isRemoved()) {
      remoteMembers.remove(member);
    }
    if (event.isAdded()) {
      remoteMembers.add(member);
    }
  }

  private void onError(Throwable throwable) {
    LOGGER.error("Received unexpected error[{}]: ", period, throwable);
  }

  // ================================================
  // ============== Helper Methods ==================
  // ================================================

  private boolean isGossipReq(Message message) {
    return GOSSIP_REQ.equals(message.qualifier());
  }

  private String generateGossipId() {
    return localMember.id() + "-" + gossipCounter++;
  }

  private void spreadGossipsTo(Member member) {
    // Select gossips to send
    List<Gossip> gossips = selectGossipsToSend(member);
    if (gossips.isEmpty()) {
      return; // nothing to spread
    }

    // Send gossip request
    Address address = member.address();

    gossips
        .stream()
        .map(this::buildGossipRequestMessage)
        .forEach(
            message ->
                transport
                    .send(address, message)
                    .subscribe(
                        null,
                        ex ->
                            LOGGER.debug(
                                "Failed to send GossipReq[{}]: {} to {}, cause: {}",
                                period,
                                message,
                                address,
                                ex.toString())));
  }

  private List<Gossip> selectGossipsToSend(Member member) {
    int periodsToSpread =
        ClusterMath.gossipPeriodsToSpread(config.getGossipRepeatMult(), remoteMembers.size() + 1);
    return gossips
        .values()
        .stream()
        .filter(
            gossipState -> gossipState.infectionPeriod() + periodsToSpread >= period) // max rounds
        .filter(gossipState -> !gossipState.isInfected(member.id())) // already infected
        .map(GossipState::gossip)
        .collect(Collectors.toList());
  }

  private List<Member> selectGossipMembers() {
    int gossipFanout = config.getGossipFanout();
    if (remoteMembers.size() < gossipFanout) { // select all
      return remoteMembers;
    } else { // select random members
      // Shuffle members initially and once reached top bound
      if (remoteMembersIndex < 0 || remoteMembersIndex + gossipFanout > remoteMembers.size()) {
        Collections.shuffle(remoteMembers);
        remoteMembersIndex = 0;
      }

      // Select members
      List<Member> selectedMembers =
          gossipFanout == 1
              ? Collections.singletonList(remoteMembers.get(remoteMembersIndex))
              : remoteMembers.subList(remoteMembersIndex, remoteMembersIndex + gossipFanout);

      // Increment index and return result
      remoteMembersIndex += gossipFanout;
      return selectedMembers;
    }
  }

  private Message buildGossipRequestMessage(Gossip gossip) {
    GossipRequest gossipRequest = new GossipRequest(gossip, localMember.id());
    return Message.withData(gossipRequest)
        .qualifier(GOSSIP_REQ)
        .sender(localMember.address())
        .build();
  }

  private void sweepGossips() {
    // Select gossips to sweep
    int periodsToSweep =
        ClusterMath.gossipPeriodsToSweep(config.getGossipRepeatMult(), remoteMembers.size() + 1);
    Set<GossipState> gossipsToRemove =
        gossips
            .values()
            .stream()
            .filter(gossipState -> period > gossipState.infectionPeriod() + periodsToSweep)
            .collect(Collectors.toSet());

    // Check if anything selected
    if (gossipsToRemove.isEmpty()) {
      return; // nothing to sweep
    }

    // Sweep gossips
    LOGGER.debug("Sweep gossips[{}]: {}", period, gossipsToRemove);
    for (GossipState gossipState : gossipsToRemove) {
      gossips.remove(gossipState.gossip().gossipId());
      MonoSink<String> sink = futures.remove(gossipState.gossip().gossipId());
      if (sink != null) {
        sink.success(gossipState.gossip().gossipId());
      }
    }
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   *
   * @return transport
   */
  Transport getTransport() {
    return transport;
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   *
   * @return local member
   */
  Member getMember() {
    return localMember;
  }
}
