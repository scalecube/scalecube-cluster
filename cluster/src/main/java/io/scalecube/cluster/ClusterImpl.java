package io.scalecube.cluster;

import static io.scalecube.cluster.fdetector.FailureDetectorImpl.PING;
import static io.scalecube.cluster.fdetector.FailureDetectorImpl.PING_ACK;
import static io.scalecube.cluster.fdetector.FailureDetectorImpl.PING_REQ;
import static io.scalecube.cluster.gossip.GossipProtocolImpl.GOSSIP_REQ;
import static io.scalecube.cluster.membership.MembershipProtocolImpl.MEMBERSHIP_GOSSIP;
import static io.scalecube.cluster.membership.MembershipProtocolImpl.SYNC;
import static io.scalecube.cluster.membership.MembershipProtocolImpl.SYNC_ACK;

import io.scalecube.cluster.fdetector.FailureDetectorImpl;
import io.scalecube.cluster.gossip.GossipProtocolImpl;
import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.membership.MembershipProtocolImpl;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.NetworkEmulator;
import io.scalecube.transport.Transport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** Cluster implementation. */
final class ClusterImpl implements Cluster {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterImpl.class);

  private static final Set<String> SYSTEM_MESSAGES =
      Collections.unmodifiableSet(
          Stream.of(PING, PING_REQ, PING_ACK, SYNC, SYNC_ACK, GOSSIP_REQ)
              .collect(Collectors.toSet()));

  private static final Set<String> SYSTEM_GOSSIPS = Collections.singleton(MEMBERSHIP_GOSSIP);

  private final ClusterConfig config;

  private final ConcurrentMap<String, Member> members = new ConcurrentHashMap<>();
  private final ConcurrentMap<Address, String> memberAddressIndex = new ConcurrentHashMap<>();

  // Disposables
  private final Disposable.Composite actionsDisposables = Disposables.composite();

  // Cluster components
  private Transport transport;
  private FailureDetectorImpl failureDetector;
  private GossipProtocolImpl gossip;
  private MembershipProtocolImpl membership;
  private Scheduler scheduler;

  public ClusterImpl(ClusterConfig config) {
    this.config = Objects.requireNonNull(config);
  }

  public Mono<Cluster> join0() {
    return Transport.bind(config.getTransportConfig())
        .flatMap(
            boundTransport -> {
              transport = boundTransport;

              // Prepare local cluster member
              Member localMember =
                  new Member(
                      IdGenerator.generateId(),
                      MembershipProtocolImpl.memberAddress(transport, config),
                      config.getMetadata());

              onMemberAdded(localMember); // store local member at this phase

              final DirectProcessor<MembershipEvent> membershipEvents = DirectProcessor.create();
              final FluxSink<MembershipEvent> membershipSink = membershipEvents.sink();

              final AtomicReference<Member> memberRef = new AtomicReference<>(localMember);

              scheduler = Schedulers.newSingle("sc-cluster-" + localMember.address().port(), true);

              failureDetector =
                  new FailureDetectorImpl(
                      memberRef::get,
                      transport,
                      membershipEvents.onBackpressureBuffer(),
                      config,
                      scheduler);

              gossip =
                  new GossipProtocolImpl(
                      memberRef::get,
                      transport,
                      membershipEvents.onBackpressureBuffer(),
                      config,
                      scheduler);

              membership =
                  new MembershipProtocolImpl(
                      memberRef, transport, failureDetector, gossip, config, scheduler);

              actionsDisposables.addAll(
                  Arrays.asList(
                      membership //
                          .listen()
                          .publishOn(scheduler)
                          .subscribe(membershipSink::next, this::onError),
                      membership
                          .listen()
                          .publishOn(scheduler)
                          .filter(MembershipEvent::isAdded)
                          .map(MembershipEvent::member)
                          .subscribe(this::onMemberAdded, this::onError),
                      membership
                          .listen()
                          .publishOn(scheduler)
                          .filter(MembershipEvent::isRemoved)
                          .map(MembershipEvent::member)
                          .subscribe(this::onMemberRemoved, this::onError),
                      membership
                          .listen()
                          .publishOn(scheduler)
                          .filter(MembershipEvent::isUpdated)
                          .map(MembershipEvent::member)
                          .subscribe(this::onMemberUpdated, this::onError)));

              failureDetector.start();
              gossip.start();

              return membership.start();
            })
        .then(Mono.just(ClusterImpl.this));
  }

  private void onError(Throwable throwable) {
    LOGGER.error("Received unexpected error: ", throwable);
  }

  private void onMemberAdded(Member member) {
    memberAddressIndex.put(member.address(), member.id());
    members.put(member.id(), member);
  }

  private void onMemberRemoved(Member member) {
    members.remove(member.id());
    memberAddressIndex.remove(member.address());
  }

  private void onMemberUpdated(Member member) {
    members.put(member.id(), member);
  }

  @Override
  public Address address() {
    return member().address();
  }

  @Override
  public Mono<Void> send(Member member, Message message) {
    return send(member.address(), message);
  }

  @Override
  public Mono<Void> send(Address address, Message message) {
    return transport.send(address, message);
  }

  @Override
  public Flux<Message> listen() {
    // filter out system messages
    return transport.listen().filter(msg -> !SYSTEM_MESSAGES.contains(msg.qualifier()));
  }

  @Override
  public Mono<String> spreadGossip(Message message) {
    return gossip.spread(message);
  }

  @Override
  public Flux<Message> listenGossips() {
    // filter out system gossips
    return gossip.listen().filter(msg -> !SYSTEM_GOSSIPS.contains(msg.qualifier()));
  }

  @Override
  public Collection<Member> members() {
    return Collections.unmodifiableCollection(members.values());
  }

  @Override
  public Member member() {
    return membership.member();
  }

  @Override
  public Optional<Member> member(String id) {
    return Optional.ofNullable(members.get(id));
  }

  @Override
  public Optional<Member> member(Address address) {
    return Optional.ofNullable(memberAddressIndex.get(address))
        .flatMap(memberId -> Optional.ofNullable(members.get(memberId)));
  }

  @Override
  public Collection<Member> otherMembers() {
    ArrayList<Member> otherMembers = new ArrayList<>(members.values());
    otherMembers.remove(membership.member());
    return Collections.unmodifiableCollection(otherMembers);
  }

  @Override
  public void updateMetadata(Map<String, String> metadata) {
    membership.updateMetadata(metadata);
  }

  @Override
  public void updateMetadataProperty(String key, String value) {
    membership.updateMetadataProperty(key, value);
  }

  @Override
  public Flux<MembershipEvent> listenMembership() {
    return membership.listen();
  }

  @Override
  public Mono<Void> shutdown() {
    return membership
        .leave()
        .doOnSubscribe(s -> LOGGER.info("Cluster member {} is shutting down", membership.member()))
        .flatMap(
            gossipId -> {
              LOGGER.info(
                  "Cluster member notified about his leaving and shutting down {}",
                  membership.member());

              // Stop accepting requests
              actionsDisposables.dispose();

              // stop algorithms
              membership.stop();
              gossip.stop();
              failureDetector.stop();

              // stop scheduler
              scheduler.dispose();

              // stop transport
              return transport.stop();
            })
        .doOnSuccess(s -> LOGGER.info("Cluster member has shut down {}", membership.member()));
  }

  @Override
  public NetworkEmulator networkEmulator() {
    return transport.networkEmulator();
  }

  @Override
  public boolean isShutdown() {
    return transport.isStopped(); // since transport is the last component stopped on shutdown
  }
}
