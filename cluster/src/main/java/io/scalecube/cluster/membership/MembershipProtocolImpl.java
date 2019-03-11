package io.scalecube.cluster.membership;

import static io.scalecube.cluster.membership.MemberStatus.ALIVE;
import static io.scalecube.cluster.membership.MemberStatus.DEAD;

import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.CorrelationIdGenerator;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.fdetector.FailureDetector;
import io.scalecube.cluster.fdetector.FailureDetectorEvent;
import io.scalecube.cluster.gossip.GossipProtocol;
import io.scalecube.cluster.metadata.MetadataStore;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
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
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Scheduler;

public final class MembershipProtocolImpl implements MembershipProtocol {

  private static final Logger LOGGER = LoggerFactory.getLogger(MembershipProtocolImpl.class);

  private enum MembershipUpdateReason {
    FAILURE_DETECTOR_EVENT,
    MEMBERSHIP_GOSSIP,
    SYNC,
    INITIAL_SYNC,
    SUSPICION_TIMEOUT
  }

  // Qualifiers

  public static final String SYNC = "sc/membership/sync";
  public static final String SYNC_ACK = "sc/membership/syncAck";
  public static final String MEMBERSHIP_GOSSIP = "sc/membership/gossip";

  private final Member localMember;

  // Injected

  private final Transport transport;
  private final MembershipConfig config;
  private final List<Address> seedMembers;
  private final FailureDetector failureDetector;
  private final GossipProtocol gossipProtocol;
  private final MetadataStore metadataStore;
  private final CorrelationIdGenerator cidGenerator;

  // State

  private final Map<String, MembershipRecord> membershipTable = new HashMap<>();
  private final Map<String, Member> members = new HashMap<>();

  // Subject

  private final FluxProcessor<MembershipEvent, MembershipEvent> subject =
      DirectProcessor.<MembershipEvent>create().serialize();
  private final FluxSink<MembershipEvent> sink = subject.sink();

  // Disposables
  private final Disposable.Composite actionsDisposables = Disposables.composite();

  // Scheduled
  private final Scheduler scheduler;
  private final Map<String, Disposable> suspicionTimeoutTasks = new HashMap<>();

  /**
   * Creates new instantiates of cluster membership protocol with given transport and config.
   *
   * @param localMember local cluster member
   * @param transport cluster transport
   * @param failureDetector failure detector
   * @param gossipProtocol gossip protocol
   * @param metadataStore metadata store
   * @param config membership config parameters
   * @param scheduler scheduler
   * @param cidGenerator correlation id generator
   */
  public MembershipProtocolImpl(
      Member localMember,
      Transport transport,
      FailureDetector failureDetector,
      GossipProtocol gossipProtocol,
      MetadataStore metadataStore,
      MembershipConfig config,
      Scheduler scheduler,
      CorrelationIdGenerator cidGenerator) {

    this.transport = Objects.requireNonNull(transport);
    this.config = Objects.requireNonNull(config);
    this.failureDetector = Objects.requireNonNull(failureDetector);
    this.gossipProtocol = Objects.requireNonNull(gossipProtocol);
    this.metadataStore = Objects.requireNonNull(metadataStore);
    this.localMember = Objects.requireNonNull(localMember);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.cidGenerator = Objects.requireNonNull(cidGenerator);

    // Prepare seeds
    seedMembers = cleanUpSeedMembers(config.getSeedMembers());

    // Init membership table with local member record
    membershipTable.put(localMember.id(), new MembershipRecord(localMember, ALIVE, 0));

    // fill in the table of members with local member
    members.put(localMember.id(), localMember);

    actionsDisposables.addAll(
        Arrays.asList(
            // Listen to incoming SYNC and SYNC ACK requests from other members
            transport
                .listen() //
                .publishOn(scheduler)
                .subscribe(this::onMessage, this::onError),

            // Listen to events from failure detector
            failureDetector
                .listen()
                .publishOn(scheduler)
                .subscribe(this::onFailureDetectorEvent, this::onError),

            // Listen to membership gossips
            gossipProtocol
                .listen()
                .publishOn(scheduler)
                .subscribe(this::onMembershipGossip, this::onError)));
  }

  // Remove duplicates and local address
  private List<Address> cleanUpSeedMembers(Collection<Address> seedMembers) {
    return new LinkedHashSet<>(seedMembers)
        .stream()
            .filter(address -> !address.equals(localMember.address()))
            .filter(address -> !address.equals(transport.address()))
            .collect(Collectors.toList());
  }

  @Override
  public Flux<MembershipEvent> listen() {
    return subject.onBackpressureBuffer();
  }

  /**
   * Updates local member incarnation number.
   *
   * @return mono handle
   */
  public Mono<Void> updateIncarnation() {
    return Mono.defer(
        () -> {
          // Update membership table
          MembershipRecord curRecord = membershipTable.get(localMember.id());
          MembershipRecord newRecord =
              new MembershipRecord(localMember, ALIVE, curRecord.incarnation() + 1);
          membershipTable.put(localMember.id(), newRecord);

          // Spread new membership record over the cluster
          return spreadMembershipGossip(newRecord);
        });
  }

  /**
   * Spreads leave notification to other cluster members.
   *
   * @return mono handle
   */
  public Mono<Void> leaveCluster() {
    return Mono.defer(
        () -> {
          MembershipRecord curRecord = membershipTable.get(localMember.id());
          MembershipRecord newRecord =
              new MembershipRecord(localMember, DEAD, curRecord.incarnation() + 1);
          membershipTable.put(localMember.id(), newRecord);
          return spreadMembershipGossip(newRecord);
        });
  }

  @Override
  public Mono<Void> start() {
    // Make initial sync with all seed members
    return Mono.create(this::start0)
        .then(Mono.fromCallable(() -> JmxMonitorMBean.start(this)))
        .then();
  }

  private void start0(MonoSink<Object> sink) {
    // In case no members at the moment just schedule periodic sync
    if (seedMembers.isEmpty()) {
      schedulePeriodicSync();
      sink.success();
      return;
    }
    // If seed addresses are specified in config - send initial sync to those nodes
    LOGGER.debug("Making initial Sync to all seed members: {}", seedMembers);

    //noinspection unchecked
    Mono<Message>[] syncs =
        seedMembers.stream()
            .map(
                address -> {
                  String cid = cidGenerator.nextCid();
                  return transport
                      .requestResponse(prepareSyncDataMsg(SYNC, cid), address)
                      .filter(this::checkSyncGroup);
                })
            .toArray(Mono[]::new);

    // Process initial SyncAck
    Flux.mergeDelayError(syncs.length, syncs)
        .take(1)
        .timeout(Duration.ofMillis(config.getSyncTimeout()), scheduler)
        .publishOn(scheduler)
        .flatMap(message -> onSyncAck(message, true))
        .doFinally(
            s -> {
              schedulePeriodicSync();
              sink.success();
            })
        .subscribe(
            null, ex -> LOGGER.info("Exception on initial SyncAck, cause: {}", ex.toString()));
  }

  @Override
  public void stop() {
    // Stop accepting requests, events and sending sync
    actionsDisposables.dispose();

    // Cancel remove members tasks
    for (String memberId : suspicionTimeoutTasks.keySet()) {
      Disposable future = suspicionTimeoutTasks.get(memberId);
      if (future != null && !future.isDisposed()) {
        future.dispose();
      }
    }
    suspicionTimeoutTasks.clear();

    // Stop publishing events
    sink.complete();
  }

  @Override
  public Collection<Member> members() {
    return new ArrayList<>(members.values());
  }

  @Override
  public Collection<Member> otherMembers() {
    return new ArrayList<>(members.values())
        .stream().filter(member -> !member.equals(localMember)).collect(Collectors.toList());
  }

  @Override
  public Member member() {
    return localMember;
  }

  @Override
  public Optional<Member> member(String id) {
    return Optional.ofNullable(members.get(id));
  }

  @Override
  public Optional<Member> member(Address address) {
    return new ArrayList<>(members.values())
        .stream().filter(member -> member.address().equals(address)).findFirst();
  }

  private void doSync() {
    Optional<Address> addressOptional = selectSyncAddress();
    if (!addressOptional.isPresent()) {
      return;
    }

    Address address = addressOptional.get();
    Message message = prepareSyncDataMsg(SYNC, null);
    LOGGER.debug("Send Sync: {} to {}", message, address);
    transport
        .send(address, message)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "Failed to send Sync: {} to {}, cause: {}", message, address, ex.toString()));
  }

  // ================================================
  // ============== Action Methods ==================
  // ================================================

  private void onMessage(Message message) {
    if (checkSyncGroup(message)) {
      if (SYNC.equals(message.qualifier())) {
        onSync(message).subscribe(null, this::onError);
      }
      if (SYNC_ACK.equals(message.qualifier())) {
        if (message.correlationId() == null) { // filter out initial sync
          onSyncAck(message, false).subscribe(null, this::onError);
        }
      }
    }
  }

  // ================================================
  // ============== Event Listeners =================
  // ================================================

  private Mono<Void> onSyncAck(Message syncAckMsg, boolean onStart) {
    return Mono.defer(
        () -> {
          LOGGER.debug("Received SyncAck: {}", syncAckMsg);
          return syncMembership(syncAckMsg.data(), onStart);
        });
  }

  /** Merges incoming SYNC data, merges it and sending back merged data with SYNC_ACK. */
  private Mono<Void> onSync(Message syncMsg) {
    return Mono.defer(
        () -> {
          LOGGER.debug("Received Sync: {}", syncMsg);
          return syncMembership(syncMsg.data(), false)
              .doOnSuccess(
                  avoid -> {
                    Message message = prepareSyncDataMsg(SYNC_ACK, syncMsg.correlationId());
                    Address address = syncMsg.sender();
                    transport
                        .send(address, message)
                        .subscribe(
                            null,
                            ex ->
                                LOGGER.debug(
                                    "Failed to send SyncAck: {} to {}, cause: {}",
                                    message,
                                    address,
                                    ex.toString()));
                  });
        });
  }

  /** Merges FD updates and processes them. */
  private void onFailureDetectorEvent(FailureDetectorEvent fdEvent) {
    MembershipRecord r0 = membershipTable.get(fdEvent.member().id());
    if (r0 == null) { // member already removed
      return;
    }
    if (r0.status() == fdEvent.status()) { // status not changed
      return;
    }
    LOGGER.debug("Received status change on failure detector event: {}", fdEvent);
    if (fdEvent.status() == ALIVE) {
      // TODO: Consider to make more elegant solution
      // Alive won't override SUSPECT so issue instead extra sync with member to force it spread
      // alive with inc + 1
      Message syncMsg = prepareSyncDataMsg(SYNC, null);
      Address address = fdEvent.member().address();
      transport
          .send(address, syncMsg)
          .subscribe(
              null,
              ex ->
                  LOGGER.debug(
                      "Failed to send {} to {}, cause: {}", syncMsg, address, ex.toString()));
    } else {
      MembershipRecord record =
          new MembershipRecord(r0.member(), fdEvent.status(), r0.incarnation());
      updateMembership(record, MembershipUpdateReason.FAILURE_DETECTOR_EVENT)
          .subscribe(null, this::onError);
    }
  }

  /** Merges received membership gossip (not spreading gossip further). */
  private void onMembershipGossip(Message message) {
    if (MEMBERSHIP_GOSSIP.equals(message.qualifier())) {
      MembershipRecord record = message.data();
      LOGGER.debug("Received membership gossip: {}", record);
      updateMembership(record, MembershipUpdateReason.MEMBERSHIP_GOSSIP)
          .subscribe(null, this::onError);
    }
  }

  private Optional<Address> selectSyncAddress() {
    List<Address> addresses =
        Stream.concat(seedMembers.stream(), otherMembers().stream().map(Member::address))
            .collect(Collectors.collectingAndThen(Collectors.toSet(), ArrayList::new));
    Collections.shuffle(addresses);
    if (addresses.isEmpty()) {
      return Optional.empty();
    } else {
      int i = ThreadLocalRandom.current().nextInt(addresses.size());
      return Optional.of(addresses.get(i));
    }
  }

  // ================================================
  // ============== Helper Methods ==================
  // ================================================

  private void onError(Throwable throwable) {
    LOGGER.error("Received unexpected error: ", throwable);
  }

  private boolean checkSyncGroup(Message message) {
    if (message.data() instanceof SyncData) {
      SyncData syncData = message.data();
      return config.getSyncGroup().equals(syncData.getSyncGroup());
    }
    return false;
  }

  private void schedulePeriodicSync() {
    int syncInterval = config.getSyncInterval();
    actionsDisposables.add(
        scheduler.schedulePeriodically(
            this::doSync, syncInterval, syncInterval, TimeUnit.MILLISECONDS));
  }

  private Message prepareSyncDataMsg(String qualifier, String cid) {
    List<MembershipRecord> membershipRecords = new ArrayList<>(membershipTable.values());
    SyncData syncData = new SyncData(membershipRecords, config.getSyncGroup());
    return Message.withData(syncData)
        .qualifier(qualifier)
        .correlationId(cid)
        .sender(localMember.address())
        .build();
  }

  private Mono<Void> syncMembership(SyncData syncData, boolean onStart) {
    return Mono.defer(
        () -> {
          MembershipUpdateReason reason =
              onStart ? MembershipUpdateReason.INITIAL_SYNC : MembershipUpdateReason.SYNC;
          return Mono.whenDelayError(
              syncData.getMembership().stream()
                  .filter(r1 -> !r1.equals(membershipTable.get(r1.id())))
                  .map(r1 -> updateMembership(r1, reason))
                  .toArray(Mono[]::new));
        });
  }

  /**
   * Try to update membership table with the given record.
   *
   * @param r1 new membership record which compares with existing r0 record
   * @param reason indicating the reason for updating membership table
   */
  private Mono<Void> updateMembership(MembershipRecord r1, MembershipUpdateReason reason) {
    return Mono.defer(
        () -> {
          Objects.requireNonNull(r1, "Membership record can't be null");
          // Get current record
          MembershipRecord r0 = membershipTable.get(r1.id());

          // Check if new record r1 overrides existing membership record r0
          if (!r1.isOverrides(r0)) {
            return Mono.empty();
          }

          // If received updated for local member then increase incarnation and spread Alive gossip
          if (r1.member().id().equals(localMember.id())) {
            int currentIncarnation = Math.max(r0.incarnation(), r1.incarnation());
            MembershipRecord r2 =
                new MembershipRecord(localMember, r0.status(), currentIncarnation + 1);

            membershipTable.put(localMember.id(), r2);

            LOGGER.debug(
                "Local membership record r0: {}, but received r1: {}, "
                    + "spread with increased incarnation r2: {}",
                r0,
                r1,
                r2);

            spreadMembershipGossip(r2)
                .subscribe(
                    null,
                    ex -> {
                      // on-op
                    });
            return Mono.empty();
          }

          // Update membership
          if (r1.isDead()) {
            membershipTable.remove(r1.id());
          } else {
            membershipTable.put(r1.id(), r1);
          }

          // Schedule/cancel suspicion timeout task
          if (r1.isSuspect()) {
            scheduleSuspicionTimeoutTask(r1);
          } else {
            cancelSuspicionTimeoutTask(r1.id());
          }

          // Emit membership and regardless of result spread gossip
          return emitMembershipEvent(r0, r1)
              .doFinally(
                  s -> {
                    // Spread gossip (unless already gossiped)
                    if (reason != MembershipUpdateReason.MEMBERSHIP_GOSSIP
                        && reason != MembershipUpdateReason.INITIAL_SYNC) {
                      spreadMembershipGossip(r1)
                          .subscribe(
                              null,
                              ex -> {
                                // no-op
                              });
                    }
                  });
        });
  }

  private Mono<Void> emitMembershipEvent(MembershipRecord r0, MembershipRecord r1) {
    return Mono.defer(
        () -> {
          final Member member = r1.member();

          if (r1.isDead()) {
            members.remove(member.id());
            // removed
            return Mono.fromRunnable(
                () -> {
                  Map<String, String> metadata = metadataStore.removeMetadata(member);
                  sink.next(MembershipEvent.createRemoved(member, metadata));
                });
          }

          if (r0 == null && r1.isAlive()) {
            members.put(member.id(), member);
            // added
            return metadataStore
                .fetchMetadata(member)
                .doOnSuccess(
                    metadata -> {
                      metadataStore.updateMetadata(member, metadata);
                      sink.next(MembershipEvent.createAdded(member, metadata));
                    })
                .onErrorResume(TimeoutException.class, e -> Mono.empty())
                .then();
          }

          if (r0 != null && r0.incarnation() < r1.incarnation()) {
            // updated
            return metadataStore
                .fetchMetadata(member)
                .doOnSuccess(
                    metadata1 -> {
                      Map<String, String> metadata0 =
                          metadataStore.updateMetadata(member, metadata1);
                      sink.next(MembershipEvent.createUpdated(member, metadata0, metadata1));
                    })
                .onErrorResume(TimeoutException.class, e -> Mono.empty())
                .then();
          }

          return Mono.empty();
        });
  }

  private void cancelSuspicionTimeoutTask(String memberId) {
    Disposable future = suspicionTimeoutTasks.remove(memberId);
    if (future != null && !future.isDisposed()) {
      future.dispose();
    }
  }

  private void scheduleSuspicionTimeoutTask(MembershipRecord record) {
    long suspicionTimeout =
        ClusterMath.suspicionTimeout(
            config.getSuspicionMult(), membershipTable.size(), config.getPingInterval());
    suspicionTimeoutTasks.computeIfAbsent(
        record.id(),
        id ->
            scheduler.schedule(
                () -> onSuspicionTimeout(id), suspicionTimeout, TimeUnit.MILLISECONDS));
  }

  private void onSuspicionTimeout(String memberId) {
    suspicionTimeoutTasks.remove(memberId);
    MembershipRecord record = membershipTable.get(memberId);
    if (record != null) {
      LOGGER.debug("Declare SUSPECTED member {} as DEAD by timeout", record);
      MembershipRecord deadRecord =
          new MembershipRecord(record.member(), DEAD, record.incarnation());
      updateMembership(deadRecord, MembershipUpdateReason.SUSPICION_TIMEOUT)
          .subscribe(null, this::onError);
    }
  }

  private Mono<Void> spreadMembershipGossip(MembershipRecord record) {
    return Mono.defer(
        () -> {
          Message msg = Message.withData(record).qualifier(MEMBERSHIP_GOSSIP).build();
          LOGGER.debug("Spead membreship: {} with gossip", msg);
          return gossipProtocol
              .spread(msg)
              .doOnError(
                  ex ->
                      LOGGER.debug(
                          "Failed to spread membership: {} with gossip, cause: {}",
                          msg,
                          ex.toString()))
              .then();
        });
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   *
   * @return failure detector
   */
  FailureDetector getFailureDetector() {
    return failureDetector;
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   *
   * @return gossip
   */
  GossipProtocol getGossipProtocol() {
    return gossipProtocol;
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
   * @return metadataStore
   */
  MetadataStore getMetadataStore() {
    return metadataStore;
  }

  /**
   * <b>NOTE:</b> this method is for testing purpose only.
   *
   * @return transport
   */
  List<MembershipRecord> getMembershipRecords() {
    return Collections.unmodifiableList(new ArrayList<>(membershipTable.values()));
  }

  public interface MonitorMBean {

    int getIncarnation();

    List<String> getAliveMembers();

    List<String> getSuspectedMembers();

    List<String> getDeadMembers();
  }

  public static class JmxMonitorMBean implements MonitorMBean {

    public static final int REMOVED_MEMBERS_HISTORY_SIZE = 42;

    private final MembershipProtocolImpl membershipProtocol;
    private final ReplayProcessor<MembershipEvent> replayProcessor;

    private JmxMonitorMBean(MembershipProtocolImpl membershipProtocol) {
      this.membershipProtocol = membershipProtocol;
      this.replayProcessor = ReplayProcessor.create(REMOVED_MEMBERS_HISTORY_SIZE);
      membershipProtocol.listen().filter(MembershipEvent::isRemoved).subscribe(replayProcessor);
    }

    private static JmxMonitorMBean start(MembershipProtocolImpl membershipProtocol)
        throws Exception {
      JmxMonitorMBean monitorMBean = new JmxMonitorMBean(membershipProtocol);
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      ObjectName objectName =
          new ObjectName(
              "io.scalecube.cluster:name=Membership@" + membershipProtocol.localMember.id());
      StandardMBean standardMBean = new StandardMBean(monitorMBean, MonitorMBean.class);
      server.registerMBean(standardMBean, objectName);
      return monitorMBean;
    }

    @Override
    public int getIncarnation() {
      Map<String, MembershipRecord> membershipTable = membershipProtocol.membershipTable;
      String localMemberId = membershipProtocol.localMember.id();
      return membershipTable.get(localMemberId).incarnation();
    }

    @Override
    public List<String> getAliveMembers() {
      return findRecordsByCondition(MembershipRecord::isAlive);
    }

    @Override
    public List<String> getSuspectedMembers() {
      return findRecordsByCondition(MembershipRecord::isSuspect);
    }

    @Override
    public List<String> getDeadMembers() {
      List<String> deadMembers = new ArrayList<>();
      replayProcessor.map(MembershipEvent::toString).subscribe(deadMembers::add);
      return deadMembers;
    }

    private List<String> findRecordsByCondition(Predicate<MembershipRecord> condition) {
      return membershipProtocol.getMembershipRecords().stream()
          .filter(condition)
          .map(record -> new Member(record.id(), record.address()))
          .map(Member::toString)
          .collect(Collectors.toList());
    }
  }
}
