package io.scalecube.cluster;

import static io.scalecube.reactor.RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED;

import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.fdetector.FailureDetectorImpl;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.gossip.GossipProtocolImpl;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.membership.MembershipProtocolImpl;
import io.scalecube.cluster.metadata.MetadataCodec;
import io.scalecube.cluster.metadata.MetadataStore;
import io.scalecube.cluster.metadata.MetadataStoreImpl;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.transport.api.TransportFactory;
import io.scalecube.net.Address;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** Cluster implementation. */
public final class ClusterImpl implements Cluster {

  private static final Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

  private static final Pattern NAMESPACE_PATTERN = Pattern.compile("^(\\w+[\\w\\-./]*\\w)+");

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
  private final Sinks.Many<MembershipEvent> membershipSink =
      Sinks.many().multicast().directBestEffort();

  // Disposables
  private final Disposable.Composite actionsDisposables = Disposables.composite();

  // Lifecycle
  private final Sinks.One<Void> start = Sinks.one();
  private final Sinks.One<Void> onStart = Sinks.one();
  private final Sinks.One<Void> shutdown = Sinks.one();
  private final Sinks.One<Void> onShutdown = Sinks.one();

  // Cluster components
  private Transport transport;
  private Member localMember;
  private FailureDetectorImpl failureDetector;
  private GossipProtocolImpl gossip;
  private MembershipProtocolImpl membership;
  private MetadataStore metadataStore;
  private Scheduler scheduler;

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
        .asMono()
        .then(doStart())
        .doOnSuccess(avoid -> onStart.emitEmpty(RETRY_NON_SERIALIZED))
        .doOnError(th -> onStart.emitError(th, RETRY_NON_SERIALIZED))
        .subscribe(null, th -> LOGGER.error("[{}][doStart] Exception occurred:", localMember, th));

    shutdown
        .asMono()
        .then(doShutdown())
        .doFinally(s -> onShutdown.emitEmpty(RETRY_NON_SERIALIZED))
        .subscribe(
            null,
            th ->
                LOGGER.warn("[{}][doShutdown] Exception occurred: {}", localMember, th.toString()));
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
   * @param supplier transport factory supplier
   * @return new {@code ClusterImpl} instance
   */
  public ClusterImpl transportFactory(Supplier<TransportFactory> supplier) {
    Objects.requireNonNull(supplier);
    ClusterImpl cluster = new ClusterImpl(this);
    cluster.config = config.transport(opts -> opts.transportFactory(supplier.get()));
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
          start.emitEmpty(RETRY_NON_SERIALIZED);
          return onStart.asMono().thenReturn(this);
        });
  }

  public Cluster startAwait() {
    return start().block();
  }

  private Mono<Cluster> doStart() {
    return Mono.fromRunnable(this::validateConfiguration).then(Mono.defer(this::doStart0));
  }

  private Mono<Cluster> doStart0() {
    return Transport.bind(config.transportConfig())
        .flatMap(
            boundTransport -> {
              localMember = createLocalMember(boundTransport.address());
              transport = new SenderAwareTransport(boundTransport, localMember.address());

              scheduler = Schedulers.newSingle("sc-cluster-" + localMember.address().port(), true);

              failureDetector =
                  new FailureDetectorImpl(
                      localMember,
                      transport,
                      membershipSink.asFlux().onBackpressureBuffer(),
                      config.failureDetectorConfig(),
                      scheduler);

              gossip =
                  new GossipProtocolImpl(
                      localMember,
                      transport,
                      membershipSink.asFlux().onBackpressureBuffer(),
                      config.gossipConfig(),
                      scheduler);

              metadataStore =
                  new MetadataStoreImpl(
                      localMember, transport, config.metadata(), config, scheduler);

              membership =
                  new MembershipProtocolImpl(
                      localMember,
                      transport,
                      failureDetector,
                      gossip,
                      metadataStore,
                      config,
                      scheduler);

              actionsDisposables.add(
                  // Retransmit inner membership events to public api layer
                  membership
                      .listen()
                      /*.publishOn(scheduler)*/
                      // Dont uncomment, already beign executed inside scalecube-cluster thread
                      .subscribe(
                          event -> membershipSink.emitNext(event, RETRY_NON_SERIALIZED),
                          ex -> LOGGER.error("[{}][membership][error] cause:", localMember, ex),
                          () -> membershipSink.emitComplete(RETRY_NON_SERIALIZED)));

              return Mono.fromRunnable(() -> failureDetector.start())
                  .then(Mono.fromRunnable(() -> gossip.start()))
                  .then(Mono.fromRunnable(() -> metadataStore.start()))
                  .then(Mono.fromRunnable(this::startHandler))
                  .then(membership.start())
                  .then();
            })
        .doOnSubscribe(s -> LOGGER.info("[{}][doStart] Starting, config: {}", localMember, config))
        .doOnSuccess(avoid -> LOGGER.info("[{}][doStart] Started", localMember))
        .thenReturn(this);
  }

  private void validateConfiguration() {
    final MetadataCodec metadataCodec =
        StreamSupport.stream(ServiceLoader.load(MetadataCodec.class).spliterator(), false)
            .findFirst()
            .orElse(null);

    if (metadataCodec == null) {
      Object metadata = config.metadata();
      if (metadata != null && !(metadata instanceof Serializable)) {
        throw new IllegalArgumentException("Invalid cluster config: metadata must be Serializable");
      }
    }

    Objects.requireNonNull(
        config.transportConfig().transportFactory(),
        "Invalid cluster config: transportFactory must be specified");

    Objects.requireNonNull(
        config.transportConfig().messageCodec(),
        "Invalid cluster config: messageCodec must be specified");

    Objects.requireNonNull(
        config.membershipConfig().namespace(),
        "Invalid cluster config: membership namespace must be specified");

    if (!NAMESPACE_PATTERN.matcher(config.membershipConfig().namespace()).matches()) {
      throw new IllegalArgumentException(
          "Invalid cluster config: membership namespace format is invalid");
    }
  }

  private void startHandler() {
    ClusterMessageHandler handler = this.handler.apply(this);
    actionsDisposables.add(
        listenMessage()
            .subscribe(
                handler::onMessage,
                ex -> LOGGER.error("[{}][onMessage][error] cause:", localMember, ex)));
    actionsDisposables.add(
        listenMembership()
            .subscribe(
                handler::onMembershipEvent,
                ex -> LOGGER.error("[{}][onMembershipEvent][error] cause:", localMember, ex)));
    actionsDisposables.add(
        listenGossip()
            .subscribe(
                handler::onGossip,
                ex -> LOGGER.error("[{}][onGossip][error] cause:", localMember, ex)));
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
    return membershipSink.asFlux().onBackpressureBuffer();
  }

  /**
   * Creates and prepares local cluster member. An address of member that's being constructed may be
   * overriden from config variables.
   *
   * @param address transport address
   * @return local cluster member with cluster address and cluster member id
   */
  private Member createLocalMember(Address address) {
    int port = Optional.ofNullable(config.externalPort()).orElse(address.port());

    // calculate local member cluster address
    Address memberAddress =
        Optional.ofNullable(config.externalHost())
            .map(host -> Address.create(host, port))
            .orElseGet(() -> Address.create(address.host(), port));

    return new Member(
        config.memberId() != null ? config.memberId() : UUID.randomUUID().toString(),
        config.memberAlias(),
        memberAddress,
        config.membershipConfig().namespace());
  }

  @Override
  public Address address() {
    return member().address();
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
    return metadataStore.metadata(member).map(this::toMetadata);
  }

  @SuppressWarnings("unchecked")
  private <T> T toMetadata(ByteBuffer buffer) {
    return (T) config.metadataCodec().deserialize(buffer);
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
    shutdown.emitEmpty(RETRY_NON_SERIALIZED);
  }

  private Mono<Void> doShutdown() {
    return Mono.defer(
        () -> {
          LOGGER.info("[{}][doShutdown] Shutting down", localMember);
          return Flux.concatDelayError(leaveCluster(), dispose(), transport.stop())
              .then()
              .doFinally(s -> scheduler.dispose())
              .doOnSuccess(avoid -> LOGGER.info("[{}][doShutdown] Shutdown", localMember));
        });
  }

  private Mono<Void> leaveCluster() {
    return membership
        .leaveCluster()
        .subscribeOn(scheduler)
        .doOnSubscribe(s -> LOGGER.info("[{}][leaveCluster] Leaving cluster", localMember))
        .doOnSuccess(s -> LOGGER.info("[{}][leaveCluster] Left cluster", localMember))
        .doOnError(
            ex ->
                LOGGER.warn(
                    "[{}][leaveCluster] Exception occurred: {}", localMember, ex.toString()))
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
    return onShutdown.asMono();
  }

  private static class SenderAwareTransport implements Transport {

    private final Transport transport;
    private final Address address;

    private SenderAwareTransport(Transport transport, Address address) {
      this.transport = Objects.requireNonNull(transport);
      this.address = Objects.requireNonNull(address);
    }

    @Override
    public Address address() {
      return transport.address();
    }

    @Override
    public Mono<Transport> start() {
      return transport.start();
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
      return Message.with(message).sender(address).build();
    }
  }
}
