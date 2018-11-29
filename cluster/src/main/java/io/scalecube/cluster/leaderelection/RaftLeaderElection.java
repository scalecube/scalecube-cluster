package io.scalecube.cluster.leaderelection;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.leaderelection.api.HeartbeatRequest;
import io.scalecube.cluster.leaderelection.api.HeartbeatResponse;
import io.scalecube.cluster.leaderelection.api.Leader;
import io.scalecube.cluster.leaderelection.api.LeaderElection;
import io.scalecube.cluster.leaderelection.api.LeaderElectionGossip;
import io.scalecube.cluster.leaderelection.api.VoteRequest;
import io.scalecube.cluster.leaderelection.api.VoteResponse;
import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public abstract class RaftLeaderElection implements LeaderElection {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftLeaderElection.class);

  private static final String LEADER_ELECTION = "leader-election";

  private static final String HEARTBEAT = "/heartbeat";

  private static final String VOTE = "/vote";

  private final StateMachine stateMachine;

  private final LogicalClock clock = new LogicalClock();

  private final AtomicReference<LogicalTimestamp> currentTerm =
      new AtomicReference<LogicalTimestamp>();

  private JobScheduler timeoutScheduler;

  private JobScheduler heartbeatScheduler;

  private String memberId;

  private final int timeout;

  private final Config config;

  private final AtomicReference<String> currentLeader = new AtomicReference<String>("");

  private String serviceName;

  private Cluster cluster;

  public String leaderId() {
    return currentLeader.get();
  }

  public State currentState() {
    return stateMachine.currentState();
  }

  public LogicalTimestamp currentTerm() {
    return this.currentTerm.get();
  }

  public Mono<Leader> leader() {
    return Mono.just(new Leader(this.memberId, this.currentLeader.get()));
  }

  protected Cluster cluster() {
    return this.cluster;
  }

  public RaftLeaderElection(String serviceName, Config config) {
    this.config = config;
    this.serviceName = serviceName;
    this.timeout =
        new Random().nextInt(config.timeout() - (config.timeout() / 2)) + (config.timeout() / 2);

    this.stateMachine = StateMachine.builder().init(State.INACTIVE)
        .addTransition(State.INACTIVE, State.FOLLOWER)
        .addTransition(State.FOLLOWER, State.CANDIDATE).addTransition(State.CANDIDATE, State.LEADER)
        .addTransition(State.LEADER, State.FOLLOWER).build();

    this.stateMachine.on(State.FOLLOWER, asFollower -> {
      LOGGER.info("member: [{}] has become: [{}].", this.memberId, stateMachine.currentState());
      heartbeatScheduler.stop();
      timeoutScheduler.start(this.timeout);
      // spread the gossip about me as follower.
      this.cluster.spreadGossip(newLeaderElectionGossip(State.FOLLOWER));
      CompletableFuture.runAsync(() -> onBecomeFollower());
    });

    this.stateMachine.on(State.CANDIDATE, asCandidate -> {
      LOGGER.info("member: [{}] has become: [{}].", this.memberId, stateMachine.currentState());
      heartbeatScheduler.stop();
      currentTerm.set(clock.tick());
      sendElectionCampaign();

      // spread the gossip about me as candidate.
      this.cluster.spreadGossip(newLeaderElectionGossip(State.CANDIDATE));
      CompletableFuture.runAsync(() -> onBecomeCandidate());
    });

    this.stateMachine.on(State.LEADER, asLeader -> {
      LOGGER.info("member: [{}] has become: [{}].", this.memberId, stateMachine.currentState());
      timeoutScheduler.stop();
      heartbeatScheduler.start(config.heartbeatInterval());
      this.currentLeader.set(this.memberId);

      // spread the gossip about me as a new leader.
      this.cluster.spreadGossip(newLeaderElectionGossip(State.LEADER));
      CompletableFuture.runAsync(() -> onBecomeLeader());
    });

    this.currentTerm.set(clock.tick());
    this.timeoutScheduler = new JobScheduler(onHeartbeatNotRecived());
    this.heartbeatScheduler = new JobScheduler(sendHeartbeat());
  }

  public RaftLeaderElection(String name) {
    this(name, Config.build());
  }

  public RaftLeaderElection(Cluster cluster, String name) {
    this(name, Config.build());
    this.cluster = cluster;
    this.cluster.updateMetadataProperty(name, LEADER_ELECTION).subscribe();
  }

  @Override
  public Mono<Void> start() {
    return Mono.create(sink -> start(this.cluster));
  }

  public void start(Cluster cluster) {

    cluster.listen().filter(m -> isHeartbeat(m.qualifier())).subscribe(request -> {
      onHeartbeat(request.data()).subscribe(resp -> {
        cluster.send(request.sender(), Message.from(request).withData(resp).build()).subscribe();
      });
    });

    cluster.listen().filter(m -> isVote(m.qualifier())).subscribe(request -> {
      onRequestVote(request).subscribe(resp -> {
        cluster.send(request.sender(), resp).subscribe();
      });
    });

    this.memberId = cluster.member().id();
    this.cluster = cluster;
    this.stateMachine.transition(State.FOLLOWER, currentTerm.get());
  }

  protected abstract void onBecomeFollower();

  protected abstract void onBecomeCandidate();

  protected abstract void onBecomeLeader();

  private Mono<HeartbeatResponse> onHeartbeat(HeartbeatRequest request) {
    LOGGER.debug("member [{}] recived heartbeat request: [{}]", this.memberId, request);
    this.timeoutScheduler.reset(this.timeout);

    LogicalTimestamp term = LogicalTimestamp.fromBytes(request.term());
    if (currentTerm.get().isBefore(term)) {
      LOGGER.info("member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.",
          this.memberId, currentTerm.get(), term);
      currentTerm.set(term);
    }

    if (!stateMachine.currentState().equals(State.FOLLOWER)) {

      LOGGER.info("member [{}] currentState [{}] and recived heartbeat. becoming FOLLOWER.",
          this.memberId, stateMachine.currentState());
      stateMachine.transition(State.FOLLOWER, term);
    }

    this.currentLeader.set(request.memberId());

    return Mono.just(new HeartbeatResponse(this.memberId, currentTerm.get().toBytes()));
  }

  private Mono<Message> onRequestVote(Message request) {

    VoteRequest voteReq = request.data();
    LogicalTimestamp term = LogicalTimestamp.fromBytes(voteReq.term());

    boolean voteGranted = currentTerm.get().isBefore(term);
    LOGGER.info("member [{}:{}] recived vote request: [{}] voteGranted: [{}].", this.memberId,
        stateMachine.currentState(), request.data(), voteGranted);

    if (currentTerm.get().isBefore(term)) {
      LOGGER.info("member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.",
          this.memberId, currentTerm.get(), term);
      currentTerm.set(term);
    }

    return Mono.just(Message.builder().correlationId(request.correlationId())
        .data(new VoteResponse(voteGranted, this.memberId)).build());
  }

  private Consumer onHeartbeatNotRecived() {
    return toCandidate -> {
      this.timeoutScheduler.stop();
      this.currentTerm.set(clock.tick(currentTerm.get()));
      this.stateMachine.transition(State.CANDIDATE, currentTerm.get());
      LOGGER.info("member: [{}] didnt recive heartbeat until timeout: [{}ms] became: [{}]",
          this.memberId, timeout, stateMachine.currentState());
    };
  }

  /**
   * find all leader election services that are remote and send current term to all of them.
   * 
   * @return consumer.
   */
  private Consumer sendHeartbeat() {

    return heartbeat -> {

      Collection<Member> services = findPeers();

      services.stream().forEach(instance -> {
        LOGGER.debug("member: [{}] sending heartbeat: [{}].", this.memberId, instance.id());

        Message request =
            asRequest(HEARTBEAT, new HeartbeatRequest(currentTerm.get().toBytes(), this.memberId));

        requestOne(request, instance.address()).subscribe(next -> {
          HeartbeatResponse response = next.data();
          LogicalTimestamp term = LogicalTimestamp.fromBytes(response.term());
          if (currentTerm.get().isBefore(term)) {
            LOGGER.info("member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.",
                this.memberId, currentTerm.get(), term);
            currentTerm.set(term);
          }
        });
      });
    };
  }

  // election requesting votes from peers and try reach consensus.
  // if most of peer members grant vote then member transition to a leader state.
  // the election process is waiting until timeout of consensusTimeout in case
  // timeout reached the member transition to Follower state.
  private void sendElectionCampaign() {
    Collection<Member> members = findPeers();

    CountDownLatch consensus = new CountDownLatch((members.size() / 2));

    members.stream().forEach(instance -> {

      LOGGER.info("member: [{}] sending vote request to: [{}].", this.memberId, instance.id());

      Message request =
          asRequest(VOTE, new VoteRequest(currentTerm.get().toBytes(), instance.id()));

      requestOne(request, instance.address()).timeout(Duration.ofMillis(config.consensusTimeout()))
          .subscribe(next -> {
            VoteResponse vote = next.data();
            LOGGER.info("member: [{}] recived vote response: [{}].", this.memberId, vote);
            if (vote.granted()) {
              consensus.countDown();
            }
          });
    });

    try {
      consensus.await(config.consensusTimeout(), TimeUnit.SECONDS);
      stateMachine.transition(State.LEADER, currentTerm);
    } catch (InterruptedException e) {
      stateMachine.transition(State.FOLLOWER, currentTerm);
    }
  }

  private Mono<Message> requestOne(final Message request, Address address) {
    Objects.requireNonNull(request.correlationId());
    return Mono.create(s -> {
      cluster.listen().filter(resp -> resp.correlationId() != null)
          .filter(resp -> resp.correlationId().equals(request.correlationId())).subscribe(m -> {
            s.success(m);
          });
      cluster.send(address, request).subscribe();
    });
  }


  private Message newLeaderElectionGossip(State state) {
    return Message.builder().header(LeaderElectionGossip.TYPE, state.toString())
        .data(new LeaderElectionGossip(this.memberId, this.currentLeader.get(),
            this.currentTerm.get().toLong(), this.cluster.address()))
        .build();
  }

  private Message asRequest(String action, Object data) {
    return Message.builder().correlationId(IdGenerator.generateId()).qualifier(serviceName + action)
        .data(data).build();
  }

  private Collection<Member> findPeers() {
    return cluster().otherMembers().stream().filter(m -> m.metadata().containsKey(serviceName))
        .filter(m -> m.metadata().get(serviceName).equals(LEADER_ELECTION))
        .collect(Collectors.toSet());

  }

  private void on(State state, Consumer func) {
    stateMachine.on(state, func);
  }

  private boolean isHeartbeat(String value) {
    return (serviceName + HEARTBEAT).equalsIgnoreCase(value);
  }

  private boolean isVote(String value) {
    return (serviceName + VOTE).equalsIgnoreCase(value);
  }
}
