package io.scalecube.cluster.membership;

import static io.scalecube.cluster.membership.MemberStatus.ALIVE;
import static io.scalecube.cluster.membership.MemberStatus.DEAD;

import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.fdetector.FailureDetector;
import io.scalecube.cluster.fdetector.FailureDetectorEvent;
import io.scalecube.cluster.gossip.GossipProtocol;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import reactor.core.publisher.Mono;
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

  // Injected

  private final Member localMember;
  private final Transport transport;
  private final MembershipConfig config;
  private final List<Address> seedMembers;
  private final FailureDetector failureDetector;
  private final GossipProtocol gossipProtocol;

  // State

  private final Map<String, MembershipRecord> membershipTable = new HashMap<>();

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
   * @param config membership config parameters
   * @param scheduler scheduler
   */
  public MembershipProtocolImpl(
      Member localMember,
      Transport transport,
      FailureDetector failureDetector,
      GossipProtocol gossipProtocol,
      MembershipConfig config,
      Scheduler scheduler) {

    this.transport = Objects.requireNonNull(transport);
    this.config = Objects.requireNonNull(config);
    this.failureDetector = Objects.requireNonNull(failureDetector);
    this.gossipProtocol = Objects.requireNonNull(gossipProtocol);
    this.localMember = Objects.requireNonNull(localMember);
    this.scheduler = Objects.requireNonNull(scheduler);

    // Prepare seeds
    seedMembers = cleanUpSeedMembers(config.getSeedMembers());

    // Init membership table with local member record
    membershipTable.put(localMember.id(), new MembershipRecord(localMember, ALIVE, 0));

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
                .filter(msg -> MEMBERSHIP_GOSSIP.equals(msg.qualifier()))
                .subscribe(this::onMembershipGossip, this::onError)));
  }

  // Remove duplicates and local address
  private List<Address> cleanUpSeedMembers(Collection<Address> seedMembers) {
    Set<Address> seedMembersSet = new HashSet<>(seedMembers); // remove duplicates
    seedMembersSet.remove(localMember.address()); // remove local address
    return Collections.unmodifiableList(new ArrayList<>(seedMembersSet));
  }

  @Override
  public Flux<MembershipEvent> listen() {
    return subject.onBackpressureBuffer();
  }

  /**
   * Returns local member.
   *
   * @return local member
   */
  public Member member() {
    return localMember;
  }

  /**
   * Updates local member incarnation number.
   *
   * @return mono handle result of {@link #updateIncarnation0()}
   */
  public Mono<Void> updateIncarnation() {
    return updateIncarnation0().subscribeOn(scheduler);
  }

  /**
   * Spreads leave notification to other cluster members.
   *
   * @return mono handle
   */
  public Mono<String> leave() {
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
    return Mono.create(
        sink -> {
          // In case no members at the moment just schedule periodic sync
          if (seedMembers.isEmpty()) {
            schedulePeriodicSync();
            sink.success();
            return;
          }

          LOGGER.debug("Making initial Sync to all seed members: {}", seedMembers);

          // Listen initial Sync Ack
          String cid = localMember.id();
          transport
              .listen()
              .filter(msg -> SYNC_ACK.equals(msg.qualifier()))
              .filter(msg -> cid.equals(msg.correlationId()))
              .filter(this::checkSyncGroup)
              .take(1)
              .timeout(Duration.ofMillis(config.getSyncTimeout()), scheduler)
              .publishOn(scheduler)
              .subscribe(
                  message -> {
                    SyncData syncData = message.data();
                    String syncGroup = syncData.getSyncGroup();
                    Collection<MembershipRecord> membership = syncData.getMembership();
                    LOGGER.info("Joined cluster '{}': {}", syncGroup, membership);

                    onSyncAck(message, true);
                    schedulePeriodicSync();
                    sink.success();
                  },
                  throwable -> {
                    LOGGER.info(
                        "Timeout getting initial SyncAck from seed members: {}", seedMembers);

                    schedulePeriodicSync();
                    sink.success();
                  });

          Message syncMsg = prepareSyncDataMsg(SYNC, cid);
          Flux.fromIterable(seedMembers)
              .flatMap(address -> transport.send(address, syncMsg))
              .subscribe(
                  null,
                  ex ->
                      LOGGER.debug(
                          "Failed to send {} from {}, cause: {}",
                          syncMsg,
                          transport.address(),
                          ex));
        });
  }

  private void onError(Throwable throwable) {
    LOGGER.error("Received unexpected error: ", throwable);
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

  // ================================================
  // ============== Action Methods ==================
  // ================================================

  private void doSync() {
    Address syncMember = selectSyncAddress();
    if (syncMember == null) {
      return;
    }
    Message syncMsg = prepareSyncDataMsg(SYNC, null);
    LOGGER.debug("Send Sync to {}: {}", syncMember, syncMsg);
    transport
        .send(syncMember, syncMsg)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "Failed to send {} from {} to {}, cause: {}",
                    syncMsg,
                    transport.address(),
                    syncMember,
                    ex));
  }

  // ================================================
  // ============== Event Listeners =================
  // ================================================

  private Mono<Void> updateIncarnation0() {
    return Mono.defer(
        () -> {
          // Update membership table
          MembershipRecord curRecord = membershipTable.get(localMember.id());
          MembershipRecord newRecord =
              new MembershipRecord(localMember, ALIVE, curRecord.incarnation() + 1);
          membershipTable.put(localMember.id(), newRecord);

          // Emit membership updated event
          sink.next(MembershipEvent.createUpdated(localMember, localMember));

          // Spread new membership record over the cluster
          return spreadMembershipGossip(newRecord).then();
        });
  }

  private void onMessage(Message message) {
    if (SYNC.equals(message.qualifier()) && checkSyncGroup(message)) {
      onSync(message);
    } else if (SYNC_ACK.equals(message.qualifier())
        && message.correlationId() == null // filter out initial sync
        && checkSyncGroup(message)) {
      onSyncAck(message, false);
    }
  }

  private void onSyncAck(Message syncAckMsg, boolean initial) {
    LOGGER.debug("Received SyncAck: {}", syncAckMsg);
    syncMembership(syncAckMsg.data(), initial);
  }

  /** Merges incoming SYNC data, merges it and sending back merged data with SYNC_ACK. */
  private void onSync(Message syncMsg) {
    LOGGER.debug("Received Sync: {}", syncMsg);
    syncMembership(syncMsg.data(), false);
    Message syncAckMsg = prepareSyncDataMsg(SYNC_ACK, syncMsg.correlationId());
    transport
        .send(syncMsg.sender(), syncAckMsg)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "Failed to send {} from {} to {}, cause: {}",
                    syncAckMsg,
                    transport.address(),
                    syncMsg.sender(),
                    ex));
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
      transport
          .send(fdEvent.member().address(), syncMsg)
          .subscribe(
              null,
              ex ->
                  LOGGER.debug(
                      "Failed to send {} from {} to {}, cause: {}",
                      syncMsg,
                      transport.address(),
                      fdEvent.member().address(),
                      ex));
    } else {
      MembershipRecord r1 = new MembershipRecord(r0.member(), fdEvent.status(), r0.incarnation());
      updateMembership(r1, MembershipUpdateReason.FAILURE_DETECTOR_EVENT);
    }
  }

  /** Merges received membership gossip (not spreading gossip further). */
  private void onMembershipGossip(Message message) {
    MembershipRecord record = message.data();
    LOGGER.debug("Received membership gossip: {}", record);
    updateMembership(record, MembershipUpdateReason.MEMBERSHIP_GOSSIP);
  }

  // ================================================
  // ============== Helper Methods ==================
  // ================================================

  private Address selectSyncAddress() {
    // TODO [AK]: During running phase it should send to both seed or not seed members (issue #38)
    return !seedMembers.isEmpty()
        ? seedMembers.get(ThreadLocalRandom.current().nextInt(seedMembers.size()))
        : null;
  }

  private boolean checkSyncGroup(Message message) {
    SyncData data = message.data();
    return config.getSyncGroup().equals(data.getSyncGroup());
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

  private void syncMembership(SyncData syncData, boolean initial) {
    for (MembershipRecord r1 : syncData.getMembership()) {
      MembershipRecord r0 = membershipTable.get(r1.id());
      if (!r1.equals(r0)) {
        MembershipUpdateReason reason =
            initial ? MembershipUpdateReason.INITIAL_SYNC : MembershipUpdateReason.SYNC;
        updateMembership(r1, reason);
      }
    }
  }

  /**
   * Try to update membership table with the given record.
   *
   * @param r1 new membership record which compares with existing r0 record
   * @param reason indicating the reason for updating membership table
   */
  private void updateMembership(MembershipRecord r1, MembershipUpdateReason reason) {
    Objects.requireNonNull(r1, "Membership record can't be null");
    // Get current record
    MembershipRecord r0 = membershipTable.get(r1.id());

    // Check if new record r1 overrides existing membership record r0
    if (!r1.isOverrides(r0)) {
      return;
    }

    // If received updated for local member then increase incarnation number and spread Alive gossip
    if (r1.member().id().equals(localMember.id())) {
      int currentIncarnation = Math.max(r0.incarnation(), r1.incarnation());
      MembershipRecord r2 = new MembershipRecord(localMember, r0.status(), currentIncarnation + 1);
      membershipTable.put(localMember.id(), r2);
      LOGGER.debug("Local membership record r0={}, but received r1={}, spread r2={}", r0, r1, r2);
      spreadMembershipGossip(r2)
          .subscribe(null, ex -> LOGGER.debug("Failed to spread membership gossip, cause: {}", ex));
      return;
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

    // Emit membership event
    if (r1.isDead()) {
      sink.next(MembershipEvent.createRemoved(r1.member()));
    } else if (r0 == null && r1.isAlive()) {
      sink.next(MembershipEvent.createAdded(r1.member()));
    } else if (r0 != null && !r0.member().equals(r1.member())) {
      sink.next(MembershipEvent.createUpdated(r0.member(), r1.member()));
    }

    // Spread gossip (unless already gossiped)
    if (reason != MembershipUpdateReason.MEMBERSHIP_GOSSIP
        && reason != MembershipUpdateReason.INITIAL_SYNC) {
      spreadMembershipGossip(r1)
          .subscribe(null, ex -> LOGGER.debug("Failed to spread membership gossip, cause: {}", ex));
    }
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
      LOGGER.debug("Declare SUSPECTED member as DEAD by timeout: {}", record);
      MembershipRecord deadRecord =
          new MembershipRecord(record.member(), DEAD, record.incarnation());
      updateMembership(deadRecord, MembershipUpdateReason.SUSPICION_TIMEOUT);
    }
  }

  private Mono<String> spreadMembershipGossip(MembershipRecord record) {
    return Mono.defer(
        () ->
            gossipProtocol.spread(
                Message //
                    .withData(record)
                    .qualifier(MEMBERSHIP_GOSSIP)
                    .build()));
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
   * @return transport
   */
  List<MembershipRecord> getMembershipRecords() {
    return Collections.unmodifiableList(new ArrayList<>(membershipTable.values()));
  }
}
