package io.scalecube.cluster.membership;

import static io.scalecube.cluster.membership.MemberStatus.ALIVE;
import static io.scalecube.cluster.membership.MemberStatus.DEAD;
import static io.scalecube.cluster.membership.MemberStatus.LEAVING;
import static io.scalecube.reactor.RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.fdetector.FailureDetector;
import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.fdetector.FailureDetectorEvent;
import io.scalecube.cluster.gossip.GossipProtocol;
import io.scalecube.cluster.metadata.MetadataStore;
import io.scalecube.cluster.monitor.ClusterMonitorModel;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.net.Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

public final class MembershipProtocolImpl implements MembershipProtocol {

  private static final Logger LOGGER = LoggerFactory.getLogger(MembershipProtocol.class);

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
  private final MembershipConfig membershipConfig;
  private final FailureDetectorConfig failureDetectorConfig;
  private final List<Address> seedMembers;
  private final FailureDetector failureDetector;
  private final GossipProtocol gossipProtocol;
  private final MetadataStore metadataStore;
  private final ClusterMonitorModel.Builder monitorModelBuilder;

  // State

  private final Map<String, MembershipRecord> membershipTable = new HashMap<>();
  private final Map<String, Member> members = new HashMap<>();
  private final List<MembershipEvent> removedMembersHistory = new CopyOnWriteArrayList<>();
  private final Set<String> aliveEmittedSet = new HashSet<>();

  // Subject

  private final Sinks.Many<MembershipEvent> sink = Sinks.many().multicast().directBestEffort();

  // Disposables
  private final Disposable.Composite actionsDisposables = Disposables.composite();
  private final Disposable.Swap disposable = Disposables.swap();

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
   * @param config cluster config parameters
   * @param scheduler scheduler
   * @param monitorModelBuilder monitor model builder
   */
  public MembershipProtocolImpl(
      Member localMember,
      Transport transport,
      FailureDetector failureDetector,
      GossipProtocol gossipProtocol,
      MetadataStore metadataStore,
      ClusterConfig config,
      Scheduler scheduler,
      ClusterMonitorModel.Builder monitorModelBuilder) {

    this.transport = Objects.requireNonNull(transport);
    this.failureDetector = Objects.requireNonNull(failureDetector);
    this.gossipProtocol = Objects.requireNonNull(gossipProtocol);
    this.metadataStore = Objects.requireNonNull(metadataStore);
    this.localMember = Objects.requireNonNull(localMember);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.monitorModelBuilder = Objects.requireNonNull(monitorModelBuilder);
    this.membershipConfig = Objects.requireNonNull(config).membershipConfig();
    this.failureDetectorConfig = Objects.requireNonNull(config).failureDetectorConfig();

    // Prepare seeds
    seedMembers = cleanUpSeedMembers(membershipConfig.seedMembers());

    // Init membership table with local member record
    membershipTable.put(localMember.id(), new MembershipRecord(localMember, ALIVE, 0));

    // fill in the table of members with local member
    members.put(localMember.id(), localMember);

    actionsDisposables.addAll(
        Arrays.asList(
            transport
                .listen() // Listen to incoming SYNC and SYNC ACK requests from other members
                .publishOn(scheduler)
                .subscribe(
                    this::onMessage,
                    ex -> LOGGER.error("[{}][onMessage][error] cause:", localMember, ex)),
            failureDetector
                .listen() // Listen to events from failure detector
                .publishOn(scheduler)
                .subscribe(
                    this::onFailureDetectorEvent,
                    ex ->
                        LOGGER.error(
                            "[{}][onFailureDetectorEvent][error] cause:", localMember, ex)),
            gossipProtocol
                .listen() // Listen to membership gossips
                .publishOn(scheduler)
                .subscribe(
                    this::onMembershipGossip,
                    ex -> LOGGER.error("[{}][onMembershipGossip][error] cause:", localMember, ex)),
            listen() // Listen removed members for monitoring
                .filter(MembershipEvent::isRemoved)
                .subscribe(this::onMemberRemoved)));
  }

  // Remove duplicates and local address(es)
  private List<Address> cleanUpSeedMembers(Collection<Address> seedMembers) {
    InetAddress localIpAddress = Address.getLocalIpAddress();

    String hostAddress = localIpAddress.getHostAddress();
    String hostName = localIpAddress.getHostName();

    Address memberAddr = localMember.address();
    Address transportAddr = transport.address();
    Address memberAddrByHostAddress = Address.create(hostAddress, memberAddr.port());
    Address transportAddrByHostAddress = Address.create(hostAddress, transportAddr.port());
    Address memberAddByHostName = Address.create(hostName, memberAddr.port());
    Address transportAddrByHostName = Address.create(hostName, transportAddr.port());

    return new LinkedHashSet<>(seedMembers)
        .stream()
            .filter(addr -> checkAddressesNotEqual(addr, memberAddr))
            .filter(addr -> checkAddressesNotEqual(addr, transportAddr))
            .filter(addr -> checkAddressesNotEqual(addr, memberAddrByHostAddress))
            .filter(addr -> checkAddressesNotEqual(addr, transportAddrByHostAddress))
            .filter(addr -> checkAddressesNotEqual(addr, memberAddByHostName))
            .filter(addr -> checkAddressesNotEqual(addr, transportAddrByHostName))
            .collect(Collectors.toList());
  }

  private boolean checkAddressesNotEqual(Address address0, Address address1) {
    if (!address0.equals(address1)) {
      return true;
    } else {
      LOGGER.warn("[{}] Filtering out seed address: {}", localMember, address0);
      return false;
    }
  }

  @Override
  public Flux<MembershipEvent> listen() {
    return sink.asFlux().onBackpressureBuffer();
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
              new MembershipRecord(localMember, LEAVING, curRecord.incarnation() + 1);
          membershipTable.put(localMember.id(), newRecord);
          return spreadMembershipGossip(newRecord);
        });
  }

  @Override
  public Mono<Void> start() {
    // Make initial sync with all seed members
    return Mono.create(this::start0).then();
  }

  private void start0(MonoSink<Object> sink) {
    // Prepare monitor model
    monitorModelBuilder
        .seedMembers(seedMembers)
        .incarnationSupplier(this::getIncarnation)
        .aliveMembersSupplier(this::getAliveMembers)
        .suspectedMembersSupplier(this::getSuspectedMembers)
        .removedMembersSupplier(this::getRemovedMembers);

    // In case no members at the moment just schedule periodic sync
    if (seedMembers.isEmpty()) {
      schedulePeriodicSync();
      sink.success();
      return;
    }
    // If seed addresses are specified in config - send initial sync to those nodes
    LOGGER.info("[{}] Making initial Sync to all seed members: {}", localMember, seedMembers);

    //noinspection unchecked
    Mono<Message>[] syncs =
        seedMembers.stream()
            .map(
                address ->
                    transport
                        .requestResponse(
                            address, prepareSyncDataMsg(SYNC, UUID.randomUUID().toString()))
                        .doOnError(
                            ex ->
                                LOGGER.warn(
                                    "[{}] Exception on initial Sync, cause: {}",
                                    localMember,
                                    ex.toString()))
                        .onErrorResume(Exception.class, e -> Mono.empty()))
            .toArray(Mono[]::new);

    // Process initial SyncAck
    Flux.mergeDelayError(syncs.length, syncs)
        .take(syncs.length)
        .timeout(Duration.ofMillis(membershipConfig.syncTimeout()), scheduler)
        .publishOn(scheduler)
        .flatMap(message -> onSyncAck(message, true))
        .doFinally(
            s -> {
              schedulePeriodicSync();
              sink.success();
            })
        .subscribe(
            null,
            ex ->
                LOGGER.warn(
                    "[{}] Exception on initial SyncAck, cause: {}", localMember, ex.toString()));
  }

  @Override
  public void stop() {
    // Stop accepting requests, events and sending sync
    actionsDisposables.dispose();
    disposable.dispose();

    // Cancel remove members tasks
    for (String memberId : suspicionTimeoutTasks.keySet()) {
      Disposable future = suspicionTimeoutTasks.get(memberId);
      if (future != null && !future.isDisposed()) {
        future.dispose();
      }
    }
    suspicionTimeoutTasks.clear();

    // Stop publishing events
    sink.emitComplete(RETRY_NON_SERIALIZED);
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
    Address address = selectSyncAddress().orElse(null);
    if (address == null) {
      return;
    }

    Message message = prepareSyncDataMsg(SYNC, null);
    LOGGER.debug("[{}][doSync] Send Sync to {}", localMember, address);
    transport
        .send(address, message)
        .subscribe(
            null,
            ex ->
                LOGGER.debug(
                    "[{}][doSync] Failed to send Sync to {}, cause: {}",
                    localMember,
                    address,
                    ex.toString()));
  }

  // ================================================
  // ============== Action Methods ==================
  // ================================================

  private void onMessage(Message message) {
    if (isSync(message)) {
      onSync(message)
          .subscribe(null, ex -> LOGGER.error("[{}][onSync][error] cause:", localMember, ex));
    } else if (isSyncAck(message)) {
      if (message.correlationId() == null) { // filter out initial sync
        onSyncAck(message, false)
            .subscribe(null, ex -> LOGGER.error("[{}][onSyncAck][error] cause:", localMember, ex));
      }
    }
  }

  private boolean isSync(Message message) {
    return SYNC.equals(message.qualifier());
  }

  private boolean isSyncAck(Message message) {
    return SYNC_ACK.equals(message.qualifier());
  }

  // ================================================
  // ============== Event Listeners =================
  // ================================================

  private Mono<Void> onSyncAck(Message syncAckMsg, boolean onStart) {
    return Mono.defer(
        () -> {
          LOGGER.debug("[{}] Received SyncAck from {}", localMember, syncAckMsg.sender());
          return syncMembership(syncAckMsg.data(), onStart);
        });
  }

  /** Merges incoming SYNC data, merges it and sending back merged data with SYNC_ACK. */
  private Mono<Void> onSync(Message syncMsg) {
    return Mono.defer(
        () -> {
          final Address sender = syncMsg.sender();
          LOGGER.debug("[{}] Received Sync from {}", localMember, sender);
          return syncMembership(syncMsg.data(), false)
              .doOnSuccess(
                  avoid -> {
                    Message message = prepareSyncDataMsg(SYNC_ACK, syncMsg.correlationId());
                    transport
                        .send(sender, message)
                        .subscribe(
                            null,
                            ex ->
                                LOGGER.debug(
                                    "[{}] Failed to send SyncAck to {}, cause: {}",
                                    localMember,
                                    sender,
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
    LOGGER.debug("[{}][onFailureDetectorEvent] Received status change: {}", localMember, fdEvent);
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
                      "[{}][onFailureDetectorEvent] Failed to send Sync to {}, cause: {}",
                      localMember,
                      address,
                      ex.toString()));
    } else {
      MembershipRecord record =
          new MembershipRecord(r0.member(), fdEvent.status(), r0.incarnation());
      updateMembership(record, MembershipUpdateReason.FAILURE_DETECTOR_EVENT)
          .subscribe(
              null,
              ex ->
                  LOGGER.error(
                      "[{}][onFailureDetectorEvent][updateMembership][error] cause:",
                      localMember,
                      ex));
    }
  }

  /** Merges received membership gossip (not spreading gossip further). */
  private void onMembershipGossip(Message message) {
    if (MEMBERSHIP_GOSSIP.equals(message.qualifier())) {
      MembershipRecord record = message.data();
      LOGGER.debug("[{}] Received membership gossip: {}", localMember, record);
      updateMembership(record, MembershipUpdateReason.MEMBERSHIP_GOSSIP)
          .subscribe(
              null,
              ex ->
                  LOGGER.error(
                      "[{}][onMembershipGossip][updateMembership][error] cause:", localMember, ex));
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

  private void schedulePeriodicSync() {
    int syncInterval = membershipConfig.syncInterval();
    disposable.update(
        scheduler.schedulePeriodically(
            this::doSync, syncInterval, syncInterval, TimeUnit.MILLISECONDS));
  }

  private Message prepareSyncDataMsg(String qualifier, String cid) {
    List<MembershipRecord> membershipRecords = new ArrayList<>(membershipTable.values());
    SyncData syncData = new SyncData(membershipRecords);
    return Message.withData(syncData).qualifier(qualifier).correlationId(cid).build();
  }

  private Mono<Void> syncMembership(SyncData syncData, boolean onStart) {
    return Mono.defer(
        () -> {
          MembershipUpdateReason reason =
              onStart ? MembershipUpdateReason.INITIAL_SYNC : MembershipUpdateReason.SYNC;

          //noinspection unchecked
          Mono<Void>[] monos =
              syncData.getMembership().stream()
                  .map(
                      r1 ->
                          updateMembership(r1, reason)
                              .doOnError(
                                  ex ->
                                      LOGGER.warn(
                                          "[{}][syncMembership][{}][error] cause: {}",
                                          localMember,
                                          reason,
                                          ex.toString()))
                              .onErrorResume(ex -> Mono.empty()))
                  .toArray(Mono[]::new);

          return Flux.mergeDelayError(monos.length, monos).then();
        });
  }

  private static boolean areNamespacesRelated(String namespace1, String namespace2) {
    Path ns1 = Paths.get(namespace1);
    Path ns2 = Paths.get(namespace2);

    if (ns1.compareTo(ns2) == 0) {
      return true;
    }

    int n1 = ns1.getNameCount();
    int n2 = ns2.getNameCount();
    if (n1 == n2) {
      return false;
    }

    Path shorter = n1 < n2 ? ns1 : ns2;
    Path longer = n1 < n2 ? ns2 : ns1;

    boolean areNamespacesRelated = true;
    for (int i = 0; i < shorter.getNameCount(); i++) {
      if (!shorter.getName(i).equals(longer.getName(i))) {
        areNamespacesRelated = false;
        break;
      }
    }
    return areNamespacesRelated;
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

          // Compare local namespace against namespace from incoming member
          String localNamespace = membershipConfig.namespace();
          String namespace = r1.member().namespace();
          if (!areNamespacesRelated(localNamespace, namespace)) {
            LOGGER.debug(
                "[{}][updateMembership][{}] Skipping update, "
                    + "namespace not matched, local: {}, inbound: {}",
                localMember,
                reason,
                localNamespace,
                namespace);
            return Mono.empty();
          }

          // Get current record
          MembershipRecord r0 = membershipTable.get(r1.member().id());

          // if current record is LEAVING then we want to process other event too
          // Check if new record r1 overrides existing membership record r0
          if ((r0 == null || !r0.isLeaving()) && !r1.isOverrides(r0)) {
            LOGGER.debug(
                "[{}][updateMembership][{}] Skipping update, "
                    + "can't override r0: {} with received r1: {}",
                localMember,
                reason,
                r0,
                r1);
            return Mono.empty();
          }

          // If received updated for local member then increase incarnation and spread Alive gossip
          if (r1.member().address().equals(localMember.address())) {
            if (r1.member().id().equals(localMember.id())) {
              return onSelfMemberDetected(r0, r1, reason);
            } else {
              return Mono.empty();
            }
          }

          if (r1.isLeaving()) {
            return onLeavingDetected(r0, r1);
          }

          if (r1.isDead()) {
            return onDeadMemberDetected(r1);
          }

          if (r1.isSuspect()) {
            // Update membership and schedule/cancel suspicion timeout task
            if (r0 == null || !r0.isLeaving()) {
              membershipTable.put(r1.member().id(), r1);
            }
            scheduleSuspicionTimeoutTask(r1);
            spreadMembershipGossipUnlessGossiped(r1, reason);
          }

          if (r1.isAlive()) {
            if (r0 != null && r0.isLeaving()) {
              return onAliveAfterLeaving(r1);
            }

            // New alive or updated alive
            if (r0 == null || r0.incarnation() < r1.incarnation()) {
              return metadataStore
                  .fetchMetadata(r1.member())
                  .doOnError(
                      ex ->
                          LOGGER.warn(
                              "[{}][updateMembership][{}] Skipping to add/update member: {}, "
                                  + "due to failed fetchMetadata call (cause: {})",
                              localMember,
                              reason,
                              r1,
                              ex.toString()))
                  .doOnSuccess(
                      metadata1 -> {
                        // If metadata was received then member is Alive
                        cancelSuspicionTimeoutTask(r1.member().id());
                        spreadMembershipGossipUnlessGossiped(r1, reason);
                        // Update membership
                        ByteBuffer metadata0 = metadataStore.updateMetadata(r1.member(), metadata1);
                        onAliveMemberDetected(r1, metadata0, metadata1);
                      })
                  .onErrorResume(Exception.class, e -> Mono.empty())
                  .then();
            }
          }

          return Mono.empty();
        });
  }

  private Mono<Void> onAliveAfterLeaving(MembershipRecord r1) {
    // r1 is outdated "ALIVE" event because we already have "LEAVING"
    final Member member = r1.member();
    final String memberId = member.id();

    members.put(memberId, member);

    // Emit events if needed and ignore alive
    if (aliveEmittedSet.add(memberId)) {
      final long timestamp = System.currentTimeMillis();

      // There is no metadata in this case
      // We could'n fetch metadata because node already wanted to leave
      publishEvent(MembershipEvent.createAdded(member, null, timestamp));
      publishEvent(MembershipEvent.createLeaving(member, null, timestamp));
    }

    return Mono.empty();
  }

  private Mono<Void> onSelfMemberDetected(
      MembershipRecord r0, MembershipRecord r1, MembershipUpdateReason reason) {
    return Mono.fromRunnable(
        () -> {
          int currentIncarnation = Math.max(r0.incarnation(), r1.incarnation());
          MembershipRecord r2 =
              new MembershipRecord(localMember, r0.status(), currentIncarnation + 1);

          membershipTable.put(localMember.id(), r2);

          LOGGER.debug(
              "[{}][updateMembership][{}] Updating incarnation, "
                  + "local record r0: {} to received r1: {}, "
                  + "spreading with increased incarnation r2: {}",
              localMember,
              reason,
              r0,
              r1,
              r2);

          spreadMembershipGossip(r2)
              .subscribe(
                  null,
                  th -> {
                    // no-op
                  });
        });
  }

  private Mono<Void> onLeavingDetected(MembershipRecord r0, MembershipRecord r1) {
    return Mono.defer(
        () -> {
          final Member member = r1.member();
          final String memberId = member.id();

          membershipTable.put(memberId, r1);

          if (r0 != null
              && (r0.isAlive() || (r0.isSuspect() && aliveEmittedSet.contains(memberId)))) {

            final ByteBuffer metadata = metadataStore.metadata(member).orElse(null);
            final long timestamp = System.currentTimeMillis();
            publishEvent(MembershipEvent.createLeaving(member, metadata, timestamp));
          }

          if (r0 == null || !r0.isLeaving()) {
            scheduleSuspicionTimeoutTask(r1);
            return spreadMembershipGossip(r1);
          } else {
            return Mono.empty();
          }
        });
  }

  private void publishEvent(MembershipEvent event) {
    LOGGER.info("[{}][publishEvent] {}", localMember, event);
    sink.emitNext(event, RETRY_NON_SERIALIZED);
  }

  private Mono<Void> onDeadMemberDetected(MembershipRecord r1) {
    return Mono.fromRunnable(
        () -> {
          final Member member = r1.member();

          cancelSuspicionTimeoutTask(member.id());

          if (!members.containsKey(member.id())) {
            return;
          }

          // Removed membership
          members.remove(member.id());
          final MembershipRecord r0 = membershipTable.remove(member.id());
          final ByteBuffer metadata = metadataStore.removeMetadata(member);
          aliveEmittedSet.remove(member.id());

          // Log that member leaved gracefully or without notification
          if (r0.isLeaving()) {
            LOGGER.info("[{}] Member leaved gracefully: {}", localMember, member);
          } else {
            LOGGER.info("[{}] Member leaved without notification: {}", localMember, member);
          }

          final long timestamp = System.currentTimeMillis();
          publishEvent(MembershipEvent.createRemoved(member, metadata, timestamp));
        });
  }

  private void onAliveMemberDetected(
      MembershipRecord r1, ByteBuffer metadata0, ByteBuffer metadata1) {

    final Member member = r1.member();

    final boolean memberExists = members.containsKey(member.id());

    final long timestamp = System.currentTimeMillis();
    MembershipEvent event = null;
    if (!memberExists) {
      event = MembershipEvent.createAdded(member, metadata1, timestamp);
    } else if (!metadata1.equals(metadata0)) {
      event = MembershipEvent.createUpdated(member, metadata0, metadata1, timestamp);
    }

    members.put(member.id(), member);
    membershipTable.put(member.id(), r1);

    if (event != null) {

      publishEvent(event);

      if (event.isAdded()) {
        aliveEmittedSet.add(member.id());
      }
    }
  }

  private void cancelSuspicionTimeoutTask(String memberId) {
    Disposable future = suspicionTimeoutTasks.remove(memberId);
    if (future != null && !future.isDisposed()) {
      LOGGER.debug("[{}] Cancelled SuspicionTimeoutTask for {}", localMember, memberId);
      future.dispose();
    }
  }

  private void scheduleSuspicionTimeoutTask(MembershipRecord r) {
    long suspicionTimeout =
        ClusterMath.suspicionTimeout(
            membershipConfig.suspicionMult(),
            membershipTable.size(),
            failureDetectorConfig.pingInterval());

    suspicionTimeoutTasks.computeIfAbsent(
        r.member().id(),
        id -> {
          LOGGER.debug(
              "[{}] Scheduled SuspicionTimeoutTask for {}, suspicionTimeout: {}",
              localMember,
              id,
              suspicionTimeout);
          return scheduler.schedule(
              () -> onSuspicionTimeout(id), suspicionTimeout, TimeUnit.MILLISECONDS);
        });
  }

  private void onSuspicionTimeout(String memberId) {
    suspicionTimeoutTasks.remove(memberId);
    MembershipRecord r = membershipTable.get(memberId);
    if (r != null) {
      LOGGER.debug("[{}] Declare SUSPECTED member {} as DEAD by timeout", localMember, r);
      MembershipRecord deadRecord = new MembershipRecord(r.member(), DEAD, r.incarnation());
      updateMembership(deadRecord, MembershipUpdateReason.SUSPICION_TIMEOUT)
          .subscribe(
              null,
              ex ->
                  LOGGER.error(
                      "[{}][onSuspicionTimeout][updateMembership][error] cause:", localMember, ex));
    }
  }

  private void spreadMembershipGossipUnlessGossiped(
      MembershipRecord r, MembershipUpdateReason reason) {
    // Spread gossip (unless already gossiped)
    if (reason != MembershipUpdateReason.MEMBERSHIP_GOSSIP
        && reason != MembershipUpdateReason.INITIAL_SYNC) {
      spreadMembershipGossip(r)
          .subscribe(
              null,
              th -> {
                // no-op
              });
    }
  }

  private Mono<Void> spreadMembershipGossip(MembershipRecord r) {
    return Mono.defer(
        () -> {
          Message msg = Message.withData(r).qualifier(MEMBERSHIP_GOSSIP).build();
          LOGGER.debug("[{}] Send membership with gossip", localMember);
          return gossipProtocol
              .spread(msg)
              .doOnError(
                  ex ->
                      LOGGER.debug(
                          "[{}] Failed to send membership with gossip, cause: {}",
                          localMember,
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

  // ===============================================================
  // ============== Helper Methods for Monitoring ==================
  // ===============================================================

  private int getIncarnation() {
    return membershipTable.get(localMember.id()).incarnation();
  }

  private List<Member> getAliveMembers() {
    return findRecordsByCondition(MembershipRecord::isAlive);
  }

  private List<Member> getSuspectedMembers() {
    return findRecordsByCondition(MembershipRecord::isSuspect);
  }

  private List<Member> getRemovedMembers() {
    return removedMembersHistory.stream().map(MembershipEvent::member).collect(Collectors.toList());
  }

  private List<Member> findRecordsByCondition(Predicate<MembershipRecord> condition) {
    return getMembershipRecords().stream()
        .filter(condition)
        .map(MembershipRecord::member)
        .collect(Collectors.toList());
  }

  private void onMemberRemoved(MembershipEvent event) {
    int s = membershipConfig.removedMembersHistorySize();
    if (s <= 0) {
      return;
    }
    removedMembersHistory.add(event);
    if (removedMembersHistory.size() > s) {
      removedMembersHistory.remove(0);
    }
  }
}
