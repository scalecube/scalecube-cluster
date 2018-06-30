package io.scalecube.cluster.gossip;

import static io.scalecube.Preconditions.checkArgument;

import io.scalecube.ThreadFactoryBuilder;
import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.membership.MembershipProtocol;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;

import org.reactivestreams.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class GossipProtocolImpl implements GossipProtocol {

  private static final Logger LOGGER = LoggerFactory.getLogger(GossipProtocolImpl.class);

  // Qualifiers

  public static final String GOSSIP_REQ = "sc/gossip/req";

  // Injected

  private final Transport transport;
  private final MembershipProtocol membership;
  private final GossipConfig config;

  // Local State

  private long period = 0;
  private long gossipCounter = 0;
  private Map<String, GossipState> gossips = new HashMap<>();
  private Map<String, CompletableFuture<String>> futures = new HashMap<>();

  private List<Member> remoteMembers = new ArrayList<>();
  private int remoteMembersIndex = -1;

  // Subscriptions

  private Disposable onMemberAddedEventSubscriber;
  private Disposable onMemberRemovedEventSubscriber;
  private Disposable onGossipRequestSubscriber;

  // Subject
  private FluxProcessor<Message, Message> subject = DirectProcessor.<Message>create().serialize();
  private FluxSink<Message> sink = subject.sink();
  // Scheduled

  private final ScheduledExecutorService executor;
  private final Scheduler scheduler;
  private ScheduledFuture<?> spreadGossipTask;

  /**
   * Creates new instance of gossip protocol with given memberId, transport and settings.
   *
   * @param transport transport
   * @param membership membership protocol
   * @param config gossip protocol settings
   */
  public GossipProtocolImpl(Transport transport, MembershipProtocol membership, GossipConfig config) {
    checkArgument(transport != null);
    checkArgument(membership != null);
    checkArgument(config != null);
    this.transport = transport;
    this.membership = membership;
    this.config = config;
    String nameFormat = "sc-gossip-" + Integer.toString(membership.member().address().port());
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());
    this.scheduler = Schedulers.fromExecutor(executor);
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   */
  Transport getTransport() {
    return transport;
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   */
  Member getMember() {
    return membership.member();
  }

  @Override
  public void start() {
    onMemberAddedEventSubscriber = membership.listen().publishOn(scheduler)
        .filter(MembershipEvent::isAdded)
        .map(MembershipEvent::member)
        .subscribe(remoteMembers::add, this::onError);

    onMemberRemovedEventSubscriber = membership.listen().publishOn(scheduler)
        .filter(MembershipEvent::isRemoved)
        .map(MembershipEvent::member)
        .subscribe(remoteMembers::remove, this::onError);

    onGossipRequestSubscriber = transport.listen().publishOn(scheduler)
        .filter(this::isGossipReq)
        .subscribe(this::onGossipReq, this::onError);

    spreadGossipTask = executor.scheduleWithFixedDelay(this::doSpreadGossip,
        config.getGossipInterval(), config.getGossipInterval(), TimeUnit.MILLISECONDS);
  }

  private void onError(Throwable throwable) {
    LOGGER.error("Received unexpected error: ", throwable);
  }

  @Override
  public void stop() {
    // Stop accepting gossip requests
    if (onMemberAddedEventSubscriber != null) {
      onMemberAddedEventSubscriber.dispose();
    }
    if (onMemberRemovedEventSubscriber != null) {
      onMemberRemovedEventSubscriber.dispose();
    }
    if (onGossipRequestSubscriber != null) {
      onGossipRequestSubscriber.dispose();
    }

    // Stop spreading gossips
    if (spreadGossipTask != null) {
      spreadGossipTask.cancel(true);
    }

    // Shutdown executor
    // TODO AK: Consider to await termination ?!
    executor.shutdown();

    // Stop publishing events
    sink.complete();
  }

  @Override
  public CompletableFuture<String> spread(Message message) {
    CompletableFuture<String> future = new CompletableFuture<>();
    executor.execute(() -> futures.put(onSpreadGossip(message), future));
    return future;
  }

  @Override
  public Flux<Message> listen() {
    return Flux.from(subject);
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
    } catch (Exception cause) {
      LOGGER.error("Exception on sending GossipReq[{}] exception: {}", period, cause.getMessage(), cause);
    }
  }

  // ================================================
  // ============== Event Listeners =================
  // ================================================

  private String onSpreadGossip(Message message) {
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

  // ================================================
  // ============== Helper Methods ==================
  // ================================================

  private boolean isGossipReq(Message message) {
    return GOSSIP_REQ.equals(message.qualifier());
  }

  private String generateGossipId() {
    return membership.member().id() + "-" + gossipCounter++;
  }

  private void spreadGossipsTo(Member member) {
    // Select gossips to send
    List<Gossip> gossipsToSend = selectGossipsToSend(member);
    if (gossipsToSend.isEmpty()) {
      return; // nothing to spread
    }

    // Send gossip request
    Message gossipReqMsg = buildGossipRequestMessage(gossipsToSend);
    transport.send(member.address(), gossipReqMsg);
  }

  private List<Gossip> selectGossipsToSend(Member member) {
    int periodsToSpread =
        ClusterMath.gossipPeriodsToSpread(config.getGossipRepeatMult(), remoteMembers.size() + 1);
    return gossips.values().stream()
        .filter(gossipState -> gossipState.infectionPeriod() + periodsToSpread >= period) // max rounds
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
      List<Member> selectedMembers = gossipFanout == 1
          ? Collections.singletonList(remoteMembers.get(remoteMembersIndex))
          : remoteMembers.subList(remoteMembersIndex, remoteMembersIndex + gossipFanout);

      // Increment index and return result
      remoteMembersIndex += gossipFanout;
      return selectedMembers;
    }
  }

  private Message buildGossipRequestMessage(List<Gossip> gossipsToSend) {
    GossipRequest gossipReqData = new GossipRequest(gossipsToSend, membership.member().id());
    return Message.withData(gossipReqData).qualifier(GOSSIP_REQ).build();
  }

  private void sweepGossips() {
    // Select gossips to sweep
    int periodsToSweep = ClusterMath.gossipPeriodsToSweep(config.getGossipRepeatMult(), remoteMembers.size() + 1);
    Set<GossipState> gossipsToRemove = gossips.values().stream()
        .filter(gossipState -> period > gossipState.infectionPeriod() + periodsToSweep)
        .collect(Collectors.toSet());

    // Check if anything selected
    if (gossipsToRemove.isEmpty()) {
      return; // nothing to sweep
    }

    // Sweep gossips
    LOGGER.debug("Sweep gossips: {}", gossipsToRemove);
    for (GossipState gossipState : gossipsToRemove) {
      gossips.remove(gossipState.gossip().gossipId());
      CompletableFuture<String> future = futures.remove(gossipState.gossip().gossipId());
      if (future != null) {
        future.complete(gossipState.gossip().gossipId());
      }
    }
  }
}
