package io.scalecube.cluster.gossip;

import static reactor.core.publisher.Sinks.EmitFailureHandler.busyLooping;

import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

public final class GossipProtocolImpl implements GossipProtocol {

  private static final Logger LOGGER = System.getLogger(GossipProtocol.class.getName());

  // Qualifiers

  public static final String GOSSIP_REQ = "sc/gossip/req";

  // Injected

  private final Member localMember;
  private final Transport transport;
  private final GossipConfig config;

  // Local State

  private long currentPeriod = 0;
  private long gossipCounter = 0;
  private final Map<String, SequenceIdCollector> sequenceIdCollectors = new HashMap<>();
  private final Map<String, GossipState> gossips = new HashMap<>();
  private final Map<String, MonoSink<String>> futures = new HashMap<>();

  private final List<Member> remoteMembers = new ArrayList<>();
  private int remoteMembersIndex = -1;

  // Disposables

  private final Disposable.Composite actionsDisposables = Disposables.composite();

  // Subject

  private final Sinks.Many<Message> sink = Sinks.many().multicast().directBestEffort();

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
                .subscribe(
                    this::onMembershipEvent,
                    ex ->
                        LOGGER.log(
                            Level.ERROR,
                            "[{0}][onMembershipEvent][error] cause:",
                            localMember,
                            ex)),
            transport
                .listen() // Listen gossip requests
                .publishOn(scheduler)
                .filter(this::isGossipRequest)
                .subscribe(
                    this::onGossipRequest,
                    ex ->
                        LOGGER.log(
                            Level.ERROR,
                            "[{0}][onGossipRequest][error] cause:",
                            localMember,
                            ex))));
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
    sink.emitComplete(busyLooping(Duration.ofSeconds(3)));
  }

  @Override
  public Mono<String> spread(Message message) {
    return Mono.just(message)
        .subscribeOn(scheduler)
        .flatMap(msg -> Mono.create(sink -> futures.put(createAndPutGossip(msg), sink)));
  }

  @Override
  public Flux<Message> listen() {
    return sink.asFlux().onBackpressureBuffer();
  }

  // ================================================
  // ============== Action Methods ==================
  // ================================================

  private void doSpreadGossip() {
    // Increment period
    long period = currentPeriod++;

    // Check segments
    checkGossipSegmentation();

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
        LOGGER.log(
            Level.DEBUG, "[{0}][{1}] Sweep gossips: {2}", localMember, period, gossipsToRemove);
        for (String gossipId : gossipsToRemove) {
          gossips.remove(gossipId);
        }
      }

      // Check spread gossips
      Set<String> gossipsThatSpread = getGossipsThatMostLikelyDisseminated(period);
      if (!gossipsThatSpread.isEmpty()) {
        LOGGER.log(
            Level.DEBUG,
            "[{0}][{1}] Most likely disseminated gossips: {2}",
            localMember,
            period,
            gossipsThatSpread);
        for (String gossipId : gossipsThatSpread) {
          MonoSink<String> sink = futures.remove(gossipId);
          if (sink != null) {
            sink.success(gossipId);
          }
        }
      }
    } catch (Exception ex) {
      LOGGER.log(
          Level.WARNING, "[{0}][{1}][doSpreadGossip] Exception occurred:", localMember, period, ex);
    }
  }

  // ================================================
  // ============== Event Listeners =================
  // ================================================

  private String createAndPutGossip(Message message) {
    final long period = this.currentPeriod;
    final Gossip gossip = createGossip(message);
    final GossipState gossipState = new GossipState(gossip, period);

    gossips.put(gossip.gossipId(), gossipState);
    ensureSequence(localMember.id()).add(gossip.sequenceId());

    return gossip.gossipId();
  }

  private void onGossipRequest(Message message) {
    final long period = this.currentPeriod;
    final GossipRequest gossipRequest = message.data();
    for (Gossip gossip : gossipRequest.gossips()) {
      GossipState gossipState = gossips.get(gossip.gossipId());
      if (ensureSequence(gossip.gossiperId()).add(gossip.sequenceId())) {
        if (gossipState == null) { // new gossip
          gossipState = new GossipState(gossip, period);
          gossips.put(gossip.gossipId(), gossipState);
          sink.emitNext(gossip.message(), busyLooping(Duration.ofSeconds(3)));
        }
      }
      if (gossipState != null) {
        gossipState.addToInfected(gossipRequest.from());
      }
    }
  }

  private void checkGossipSegmentation() {
    final int intervalsThreshold = config.gossipSegmentationThreshold();
    for (Entry<String, SequenceIdCollector> entry : sequenceIdCollectors.entrySet()) {
      // Size of sequenceIdCollector could grow only if we never received some messages.
      // Which is possible only if current node wasn't available(suspected) for some time
      // or network issue
      final SequenceIdCollector sequenceIdCollector = entry.getValue();
      if (sequenceIdCollector.size() > intervalsThreshold) {
        LOGGER.log(
            Level.WARNING,
            "[{0}][{1}] Too many missed gossip messages from original gossiper: {2}, "
                + "current node({3}) was SUSPECTED much for a long time or connection problem",
            localMember,
            currentPeriod,
            entry.getKey(),
            localMember);

        sequenceIdCollector.clear();
      }
    }
  }

  private void onMembershipEvent(MembershipEvent event) {
    Member member = event.member();
    if (event.isRemoved()) {
      boolean removed = remoteMembers.remove(member);
      sequenceIdCollectors.remove(member.id());
      if (removed) {
        LOGGER.log(
            Level.DEBUG,
            "[{0}][{1}] Removed {2} from remoteMembers list (size={3})",
            localMember,
            currentPeriod,
            member,
            remoteMembers.size());
      }
    }
    if (event.isAdded()) {
      remoteMembers.add(member);
      LOGGER.log(
          Level.DEBUG,
          "[{0}][{1}] Added {2} to remoteMembers list (size={3})",
          localMember,
          currentPeriod,
          member,
          remoteMembers.size());
    }
  }

  // ================================================
  // ============== Helper Methods ==================
  // ================================================

  private boolean isGossipRequest(Message message) {
    return GOSSIP_REQ.equals(message.qualifier());
  }

  private Gossip createGossip(Message message) {
    return new Gossip(localMember.id(), message, gossipCounter++);
  }

  private SequenceIdCollector ensureSequence(String key) {
    return sequenceIdCollectors.computeIfAbsent(key, s -> new SequenceIdCollector());
  }

  private void spreadGossipsTo(long period, Member member) {
    // Select gossips to send
    List<Gossip> gossips = selectGossipsToSend(period, member);
    if (gossips.isEmpty()) {
      return; // nothing to spread
    }

    // Send gossip request
    String address = member.address();

    gossips.stream()
        .map(this::buildGossipRequestMessage)
        .forEach(
            message ->
                transport
                    .send(address, message)
                    .subscribe(
                        null,
                        ex ->
                            LOGGER.log(
                                Level.DEBUG,
                                "[{0}][{1}] Failed to send GossipReq({2}) to {3}, cause: {4}",
                                localMember,
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
}
