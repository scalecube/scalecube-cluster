package io.scalecube.cluster;

import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.fdetector.FailureDetectorImpl;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.gossip.GossipProtocolImpl;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.membership.MembershipProtocolImpl;
import io.scalecube.cluster.metadata.MetadataStore;
import io.scalecube.cluster.metadata.MetadataStoreImpl;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.TransportImpl;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
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
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** Cluster implementation. */
public final class ClusterImpl implements Cluster {

  private static final Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

  private static final Set<String> SYSTEM_MESSAGES =
      Collections.unmodifiableSet(
          Stream.of(
                  FailureDetectorImpl.PING,
                  FailureDetectorImpl.PING_REQ,
                  FailureDetectorImpl.PING_ACK,
                  MembershipProtocolImpl.SYNC,
                  MembershipProtocolImpl.SYNC_ACK,
                  GossipProtocolImpl.GOSSIP_REQ,
                  MetadataStoreImpl.GET_METADATA_REQ,
                  MetadataStoreImpl.GET_METADATA_RESP)
              .collect(Collectors.toSet()));

  private static final Set<String> SYSTEM_GOSSIPS =
      Collections.singleton(MembershipProtocolImpl.MEMBERSHIP_GOSSIP);

  private ClusterConfig config;
  private Function<Cluster, ? extends ClusterMessageHandler> handler =
      cluster -> new ClusterMessageHandler() {};

  // Subject
  private final DirectProcessor<MembershipEvent> membershipEvents = DirectProcessor.create();
  private final FluxSink<MembershipEvent> membershipSink = membershipEvents.sink();

  // Disposables
  private final Disposable.Composite actionsDisposables = Disposables.composite();

  // Lifecycle
  private final MonoProcessor<Void> start = MonoProcessor.create();
  private final MonoProcessor<Void> onStart = MonoProcessor.create();
  private final MonoProcessor<Void> shutdown = MonoProcessor.create();
  private final MonoProcessor<Void> onShutdown = MonoProcessor.create();

  // Cluster components
  private Transport transport;
  private Member localMember;
  private FailureDetectorImpl failureDetector;
  private GossipProtocolImpl gossip;
  private MembershipProtocolImpl membership;
  private MetadataStore metadataStore;
  private Scheduler scheduler;
  private CorrelationIdGenerator cidGenerator;

  public ClusterImpl() {
    this(ClusterConfig.defaultConfig());
  }

  public ClusterImpl(ClusterConfig config) {
    this.config = Objects.requireNonNull(config);
    initLifecycle();
  }

  private ClusterImpl(ClusterImpl that) {
    this.config = that.config.clone();
    this.handler = that.handler;
    initLifecycle();
  }

  private void initLifecycle() {
    start
        .then(doStart())
        .doOnSuccess(avoid -> onStart.onComplete())
        .doOnError(onStart::onError)
        .subscribe(
            null,
            th -> {
              LOGGER.error("Cluster member {} failed on start: ", localMember, th);
              shutdown.onComplete();
            });

    shutdown //
        .then(doShutdown())
        .doFinally(s -> onShutdown.onComplete())
        .subscribe();
  }

  /**
   * Returns a new cluster's instance which will apply the given options.
   *
   * @param options cluster config options
   * @return new {@code ClusterImpl} instance
   */
  public ClusterImpl config(UnaryOperator<ClusterConfig> options) {
    Objects.requireNonNull(options);
    ClusterImpl cluster = new ClusterImpl(this);
    cluster.config = options.apply(config);
    return cluster;
  }

  /**
   * Returns a new cluster's instance which will apply the given options.
   *
   * @param options transport config options
   * @return new {@code ClusterImpl} instance
   */
  public ClusterImpl transport(UnaryOperator<TransportConfig> options) {
    Objects.requireNonNull(options);
    ClusterImpl cluster = new ClusterImpl(this);
    cluster.config = config.transport(options);
    return cluster;
  }

  /**
   * Returns a new cluster's instance which will apply the given options.
   *
   * @param options failureDetector config options
   * @return new {@code ClusterImpl} instance
   */
  public ClusterImpl failureDetector(UnaryOperator<FailureDetectorConfig> options) {
    Objects.requireNonNull(options);
    ClusterImpl cluster = new ClusterImpl(this);
    cluster.config = config.failureDetector(options);
    return cluster;
  }

  /**
   * Returns a new cluster's instance which will apply the given options.
   *
   * @param options gossip config options
   * @return new {@code ClusterImpl} instance
   */
  public ClusterImpl gossip(UnaryOperator<GossipConfig> options) {
    Objects.requireNonNull(options);
    ClusterImpl cluster = new ClusterImpl(this);
    cluster.config = config.gossip(options);
    return cluster;
  }

  /**
   * Returns a new cluster's instance which will apply the given options.
   *
   * @param options membership config options
   * @return new {@code ClusterImpl} instance
   */
  public ClusterImpl membership(UnaryOperator<MembershipConfig> options) {
    Objects.requireNonNull(options);
    ClusterImpl cluster = new ClusterImpl(this);
    cluster.config = config.membership(options);
    return cluster;
  }

  /**
   * Returns a new cluster's instance with given handler. The previous handler will be replaced.
   *
   * @param handler message handler supplier by the cluster
   * @return new {@code ClusterImpl} instance
   */
  public ClusterImpl handler(Function<Cluster, ClusterMessageHandler> handler) {
    Objects.requireNonNull(handler);
    ClusterImpl cluster = new ClusterImpl(this);
    cluster.handler = handler;
    return cluster;
  }

  /**
   * Starts this instance. See {@link ClusterImpl#doStart()} function.
   *
   * @return mono result
   */
  public Mono<Cluster> start() {
    return Mono.defer(
        () -> {
          start.onComplete();
          return onStart.thenReturn(this);
        });
  }

  public Cluster startAwait() {
    return start().block();
  }

  private Mono<Cluster> doStart() {
    return Mono.defer(
        () -> {
          validateConfiguration();
          return doStart0();
        });
  }

  private Mono<Cluster> doStart0() {
    return TransportImpl.bind(config.transportConfig())
        .flatMap(
            transport1 -> {
              localMember = createLocalMember(transport1.address().port());
              transport = new SenderAwareTransport(transport1, localMember.address());

              cidGenerator = new CorrelationIdGenerator(localMember.id());
              scheduler = Schedulers.newSingle("sc-cluster-" + localMember.address().port(), true);

              failureDetector =
                  new FailureDetectorImpl(
                      localMember,
                      transport,
                      membershipEvents.onBackpressureBuffer(),
                      config.failureDetectorConfig(),
                      scheduler,
                      cidGenerator);

              gossip =
                  new GossipProtocolImpl(
                      localMember,
                      transport,
                      membershipEvents.onBackpressureBuffer(),
                      config.gossipConfig(),
                      scheduler);

              metadataStore =
                  new MetadataStoreImpl(
                      localMember, transport, config.metadata(), config, scheduler, cidGenerator);

              membership =
                  new MembershipProtocolImpl(
                      localMember,
                      transport,
                      failureDetector,
                      gossip,
                      metadataStore,
                      config,
                      scheduler,
                      cidGenerator);

              actionsDisposables.add(
                  membership
                      .listen()
                      /*.publishOn(scheduler)*/
                      // dont uncomment, already beign executed inside sc-cluster thread
                      .subscribe(membershipSink::next, this::onError));

              return Mono.fromRunnable(() -> failureDetector.start())
                  .then(Mono.fromRunnable(() -> gossip.start()))
                  .then(Mono.fromRunnable(() -> metadataStore.start()))
                  .then(Mono.fromRunnable(this::startHandler))
                  .then((membership.start()))
                  .then(Mono.fromCallable(() -> JmxMonitorMBean.start(this)));
            })
        .thenReturn(this);
  }

  private void validateConfiguration() {
    Objects.requireNonNull(
        config.metadataDecoder(), "Invalid cluster config: metadataDecoder must be specified");
    Objects.requireNonNull(
        config.metadataEncoder(), "Invalid cluster config: metadataEncoder must be specified");

    Objects.requireNonNull(
        config.transportConfig().messageCodec(),
        "Invalid cluster config: transport.messageCodec must be specified");

    Objects.requireNonNull(
        config.membershipConfig().syncGroup(),
        "Invalid cluster config: membership.syncGroup must be specified");
  }

  private void startHandler() {
    ClusterMessageHandler handler = this.handler.apply(this);
    actionsDisposables.add(listenMessage().subscribe(handler::onMessage, this::onError));
    actionsDisposables.add(listenMembership().subscribe(handler::onMembershipEvent, this::onError));
    actionsDisposables.add(listenGossip().subscribe(handler::onGossip, this::onError));
  }

  private void onError(Throwable th) {
    LOGGER.error("Received unexpected error: ", th);
  }

  private Flux<Message> listenMessage() {
    // filter out system messages
    return transport.listen().filter(msg -> !SYSTEM_MESSAGES.contains(msg.qualifier()));
  }

  private Flux<Message> listenGossip() {
    // filter out system gossips
    return gossip.listen().filter(msg -> !SYSTEM_GOSSIPS.contains(msg.qualifier()));
  }

  private Flux<MembershipEvent> listenMembership() {
    // listen on live stream
    return membershipEvents.onBackpressureBuffer();
  }

  /**
   * Creates and prepares local cluster member. An address of member that's being constructed may be
   * overriden from config variables.
   *
   * @param listenPort transport listen port
   * @return local cluster member with cluster address and cluster member id
   */
  private Member createLocalMember(int listenPort) {
    String localAddress = Address.getLocalIpAddress().getHostAddress();
    Integer port = Optional.ofNullable(config.memberPort()).orElse(listenPort);

    // calculate local member cluster address
    Address memberAddress =
        Optional.ofNullable(config.memberHost())
            .map(memberHost -> Address.create(memberHost, port))
            .orElseGet(() -> Address.create(localAddress, listenPort));

    return new Member(memberAddress);
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
  public Mono<Message> requestResponse(Address address, Message request) {
    return transport.requestResponse(address, request);
  }

  @Override
  public Mono<Message> requestResponse(Member member, Message request) {
    return transport.requestResponse(member.address(), request);
  }

  @Override
  public Mono<String> spreadGossip(Message message) {
    return gossip.spread(message);
  }

  @Override
  public Collection<Member> members() {
    return membership.members();
  }

  @Override
  public Collection<Member> otherMembers() {
    return membership.otherMembers();
  }

  @Override
  public <T> Optional<T> metadata() {
    return metadataStore.metadata();
  }

  @Override
  public <T> Optional<T> metadata(Member member) {
    if (member().equals(member)) {
      return metadata();
    }
    return metadataStore
        .metadata(member)
        .map(
            byteBuffer -> {
              //noinspection unchecked
              return (T) config.metadataDecoder().decode(byteBuffer);
            });
  }

  @Override
  public Member member() {
    return localMember;
  }

  @Override
  public Optional<Member> member(String id) {
    return membership.member(id);
  }

  @Override
  public Optional<Member> member(Address address) {
    return membership.member(address);
  }

  @Override
  public <T> Mono<Void> updateMetadata(T metadata) {
    return Mono.fromRunnable(() -> metadataStore.updateMetadata(metadata))
        .then(membership.updateIncarnation())
        .subscribeOn(scheduler);
  }

  @Override
  public void shutdown() {
    shutdown.onComplete();
  }

  private Mono<Void> doShutdown() {
    return Mono.defer(
        () -> {
          LOGGER.info("Cluster member {} is shutting down", localMember);
          return Flux.concatDelayError(leaveCluster(), dispose(), transport.stop())
              .then()
              .doFinally(s -> scheduler.dispose())
              .doOnSuccess(
                  avoid -> LOGGER.info("Cluster member {} has been shut down", localMember))
              .doOnError(
                  th ->
                      LOGGER.warn(
                          "Cluster member {} failed on shutdown: {}", localMember, th.toString()));
        });
  }

  private Mono<Void> leaveCluster() {
    return membership
        .leaveCluster()
        .subscribeOn(scheduler)
        .doOnSuccess(s -> LOGGER.debug("Cluster member {} has left a cluster", localMember))
        .doOnError(
            ex ->
                LOGGER.info(
                    "Cluster member {} failed on leaveCluster: {}", localMember, ex.toString()))
        .then();
  }

  private Mono<Void> dispose() {
    return Mono.fromRunnable(
        () -> {
          // Stop accepting requests
          actionsDisposables.dispose();

          // stop algorithms
          metadataStore.stop();
          membership.stop();
          gossip.stop();
          failureDetector.stop();
        });
  }

  @Override
  public Mono<Void> onShutdown() {
    return onShutdown;
  }

  @Override
  public boolean isShutdown() {
    return onShutdown.isDisposed();
  }

  public interface MonitorMBean {

    Collection<String> getMemberId();

    String getMemberIdAsString();

    Collection<String> getMetadata();

    String getMetadataAsString();
  }

  public static class JmxMonitorMBean implements MonitorMBean {

    private final ClusterImpl cluster;

    private JmxMonitorMBean(ClusterImpl cluster) {
      this.cluster = cluster;
    }

    private static JmxMonitorMBean start(ClusterImpl cluster) throws Exception {
      JmxMonitorMBean monitorMBean = new JmxMonitorMBean(cluster);
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      StandardMBean standardMBean = new StandardMBean(monitorMBean, MonitorMBean.class);
      ObjectName objectName =
          new ObjectName("io.scalecube.cluster:name=Cluster@" + cluster.member().id());
      server.registerMBean(standardMBean, objectName);
      return monitorMBean;
    }

    @Override
    public Collection<String> getMemberId() {
      return Collections.singleton(cluster.member().id());
    }

    @Override
    public String getMemberIdAsString() {
      return getMemberId().iterator().next();
    }

    @Override
    public Collection<String> getMetadata() {
      return Collections.singletonList(
          String.valueOf(cluster.metadataStore.metadata().map(Object::toString).orElse(null)));
    }

    @Override
    public String getMetadataAsString() {
      return getMetadata().iterator().next();
    }
  }

  private static class SenderAwareTransport implements Transport {

    private final Transport transport;
    private final Address sender;

    private SenderAwareTransport(Transport transport, Address sender) {
      this.transport = Objects.requireNonNull(transport);
      this.sender = Objects.requireNonNull(sender);
    }

    @Override
    public Address address() {
      return transport.address();
    }

    @Override
    public Mono<Void> stop() {
      return transport.stop();
    }

    @Override
    public boolean isStopped() {
      return transport.isStopped();
    }

    @Override
    public Mono<Void> send(Address address, Message message) {
      return Mono.defer(() -> transport.send(address, enhanceWithSender(message)));
    }

    @Override
    public Mono<Message> requestResponse(Address address, Message request) {
      return Mono.defer(() -> transport.requestResponse(address, enhanceWithSender(request)));
    }

    @Override
    public Flux<Message> listen() {
      return transport.listen();
    }

    private Message enhanceWithSender(Message message) {
      return Message.with(message).sender(sender).build();
    }
  }
}
