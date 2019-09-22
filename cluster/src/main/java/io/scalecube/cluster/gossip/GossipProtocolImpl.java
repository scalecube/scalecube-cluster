package io.scalecube.cluster.gossip;

import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.net.Address;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(GossipProtocol.class);

  // Qualifiers

  public static final String GOSSIP_REQ = "sc/gossip/req";

  // Injected

  private final Member localMember;
  private final Transport transport;
  private final GossipConfig config;

  // Local State

  private long currentPeriod = 0;
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
            membershipProcessor // Listen membership events to update remoteMembers
                .publishOn(scheduler)
                .subscribe(this::onMemberEvent, this::onError),
            transport
                .listen() // Listen gossip requests
                .publishOn(scheduler)
                .filter(this::isGossipReq)
                .subscribe(this::onGossipReq, this::onError)));
  }

  @Override
  public void start() {
    actionsDisposables.add(
        scheduler.schedulePeriodically(
            this::doSpreadGossip,
            config.gossipInterval(),
            config.gossipInterval(),
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
    return Mono.just(message)
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
    long period = currentPeriod++;

    // Check any gossips exists
    if (gossips.isEmpty()) {
      return; // nothing to spread
    }

    try {
      // Spread gossips to randomly selected member(s)
      selectGossipMembers().forEach(member -> spreadGossipsTo(period, member));

      // Sweep gossips
      Set<String> gossipsToRemove = getGossipsToRemove(period);
      if (!gossipsToRemove.isEmpty()) {
        LOGGER.debug("Sweep gossips[{}]: {}", period, gossipsToRemove);
        for (String gossipId : gossipsToRemove) {
          gossips.remove(gossipId);
        }
      }

      // Check spread gossips
      Set<String> gossipsThatSpread = getGossipsThatMostLikelyDisseminated(period);
      if (!gossipsThatSpread.isEmpty()) {
        LOGGER.debug("Most likely disseminated gossips[{}]: {}", period, gossipsThatSpread);
        for (String gossipId : gossipsThatSpread) {
          MonoSink<String> sink = futures.remove(gossipId);
          if (sink != null) {
            sink.success(gossipId);
          }
        }
      }
    } catch (Exception ex) {
      LOGGER.warn("Exception at doSpreadGossip[{}]: ", period, ex);
    }
  }

  // ================================================
  // ============== Event Listeners =================
  // ================================================

  private String createAndPutGossip(Message message) {
    long period = this.currentPeriod;
    Gossip gossip = new Gossip(generateGossipId(), message);
    GossipState gossipState = new GossipState(gossip, period);
    gossips.put(gossip.gossipId(), gossipState);
    return gossip.gossipId();
  }

  private void onGossipReq(Message message) {
    long period = this.currentPeriod;
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
      boolean removed = remoteMembers.remove(member);
      if (removed) {
        LOGGER.debug("Removed {} from remoteMembers list (size={})", member, remoteMembers.size());
      }
    }
    if (event.isAdded()) {
      remoteMembers.add(member);
      LOGGER.debug("Added {} to remoteMembers list (size={})", member, remoteMembers.size());
    }
  }

  private void onError(Throwable throwable) {
    LOGGER.error("Received unexpected error: ", throwable);
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

  private void spreadGossipsTo(long period, Member member) {
    // Select gossips to send
    List<Gossip> gossips = selectGossipsToSend(period, member);
    if (gossips.isEmpty()) {
      return; // nothing to spread
    }

    // Send gossip request
    Address address = member.address();

    gossips.stream()
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

  private List<Gossip> selectGossipsToSend(long period, Member member) {
    int periodsToSpread =
        ClusterMath.gossipPeriodsToSpread(config.gossipRepeatMult(), remoteMembers.size() + 1);
    return gossips.values().stream()
        .filter(
            gossipState -> gossipState.infectionPeriod() + periodsToSpread >= period) // max rounds
        .filter(gossipState -> !gossipState.isInfected(member.id())) // already infected
        .map(GossipState::gossip)
        .collect(Collectors.toList());
  }

  private List<Member> selectGossipMembers() {
    int gossipFanout = config.gossipFanout();
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
    return Message.withData(gossipRequest).qualifier(GOSSIP_REQ).build();
  }

  private Set<String> getGossipsToRemove(long period) {
    // Select gossips to sweep
    int periodsToSweep =
        ClusterMath.gossipPeriodsToSweep(config.gossipRepeatMult(), remoteMembers.size() + 1);
    return gossips.values().stream()
        .filter(gossipState -> period > gossipState.infectionPeriod() + periodsToSweep)
        .map(gossipState -> gossipState.gossip().gossipId())
        .collect(Collectors.toSet());
  }

  private Set<String> getGossipsThatMostLikelyDisseminated(long period) {
    // Select gossips to spread
    int periodsToSpread =
        ClusterMath.gossipPeriodsToSpread(config.gossipRepeatMult(), remoteMembers.size() + 1);
    return gossips.values().stream()
        .filter(gossipState -> period > gossipState.infectionPeriod() + periodsToSpread)
        .map(gossipState -> gossipState.gossip().gossipId())
        .collect(Collectors.toSet());
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
