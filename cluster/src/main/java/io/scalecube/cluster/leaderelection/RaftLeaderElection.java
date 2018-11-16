package io.scalecube.cluster.leaderelection;

import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.leaderelection.api.HeartbeatRequest;
import io.scalecube.cluster.leaderelection.api.HeartbeatResponse;
import io.scalecube.cluster.leaderelection.api.Leader;
import io.scalecube.cluster.leaderelection.api.LeaderElectionGossip;
import io.scalecube.cluster.leaderelection.api.LeaderElectionService;
import io.scalecube.cluster.leaderelection.api.VoteRequest;
import io.scalecube.cluster.leaderelection.api.VoteResponse;
import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import reactor.core.publisher.Mono;

public abstract class RaftLeaderElection {

  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionService.class);

  private final StateMachine stateMachine;

  private Cluster cluster;

  private final LogicalClock clock = new LogicalClock();

  private final AtomicReference<LogicalTimestamp> currentTerm = new AtomicReference<LogicalTimestamp>();

  private final JobScheduler timeoutScheduler;

  private final JobScheduler heartbeatScheduler;

  private String memberId;

  private final int timeout;

  private final Config config;

  private final AtomicReference<String> currentLeader = new AtomicReference<String>("");

  private String serviceName;

  public String leaderId() {
    return currentLeader.get();
  }
  
  public LogicalTimestamp currentTerm() {
    return this.currentTerm.get();
  }
  
  protected Cluster cluster() {
    return this.cluster;
  }
  
  public RaftLeaderElection(String name, Config config) {
    this.config = config;
    this.timeout = new Random().nextInt(config.timeout() - (config.timeout() / 2)) + (config.timeout() / 2);
    this.serviceName = name;
    this.stateMachine = StateMachine.builder()
        .init(State.INACTIVE)
        .addTransition(State.INACTIVE, State.FOLLOWER)
        .addTransition(State.FOLLOWER, State.CANDIDATE)
        .addTransition(State.CANDIDATE, State.LEADER)
        .addTransition(State.LEADER, State.FOLLOWER)
        .build();

    this.stateMachine.on(State.FOLLOWER, becomeFollower());
    this.stateMachine.on(State.CANDIDATE, becomeCandidate());
    this.stateMachine.on(State.LEADER, becomeLeader());

    this.currentTerm.set(clock.tick());
    this.timeoutScheduler = new JobScheduler(onHeartbeatNotRecived());
    this.heartbeatScheduler = new JobScheduler(sendHeartbeat());
  }

  protected void start(Cluster cluster) {
    this.memberId = cluster.member().id();
    this.cluster = cluster;
    this.stateMachine.transition(State.FOLLOWER, currentTerm.get());
  }

  public Mono<Leader> leader() {
    return Mono.just(new Leader(this.memberId, this.currentLeader.get()));
  }

  public Mono<HeartbeatResponse> onHeartbeat(HeartbeatRequest request) {
    LOGGER.debug("service: [{}] member [{}] recived heartbeat request: [{}]", serviceName, this.memberId, request);
    this.timeoutScheduler.reset(this.timeout);
    
    LogicalTimestamp term = LogicalTimestamp.fromBytes(request.term());
    if (currentTerm.get().isBefore(term)) {
      LOGGER.info("service: [{}] member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.", serviceName,
          this.memberId,
          currentTerm.get(), term);
      currentTerm.set(term);
    }

    if (!stateMachine.currentState().equals(State.FOLLOWER)) {

      LOGGER.info("service: [{}] member [{}] currentState [{}] and recived heartbeat. becoming FOLLOWER.", serviceName,
          this.memberId,
          stateMachine.currentState());
      stateMachine.transition(State.FOLLOWER, term);
    }
    

    this.currentLeader.set(request.memberId());

    return Mono.just(new HeartbeatResponse(this.memberId, currentTerm.get().toBytes()));
  }


  public Mono<VoteResponse> onRequestVote(VoteRequest request) {

    LogicalTimestamp term = LogicalTimestamp.fromBytes(request.term());

    boolean voteGranted = currentTerm.get().isBefore(term);
    LOGGER.info("service: [{}] member [{}:{}] recived vote request: [{}] voteGranted: [{}].", serviceName,
        this.memberId,
        stateMachine.currentState(), request, voteGranted);

    if (currentTerm.get().isBefore(term)) {
      LOGGER.info("service: [{}] member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.", serviceName,
          this.memberId,
          currentTerm.get(), term);
      currentTerm.set(term);
    }

    return Mono.just(new VoteResponse(voteGranted, this.memberId));
  }

  private Consumer onHeartbeatNotRecived() {
    return toCandidate -> {
      this.timeoutScheduler.stop();
      this.currentTerm.set(clock.tick(currentTerm.get()));
      this.stateMachine.transition(State.CANDIDATE, currentTerm.get());
      LOGGER.info("service: [{}] member: [{}] didnt recive heartbeat until timeout: [{}ms] became: [{}]", serviceName,
          this.memberId, timeout,
          stateMachine.currentState());
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

      services.stream()
      .forEach(instance -> {
        LOGGER.debug("service: [{}] member: [{}] sending heartbeat: [{}].", serviceName, this.memberId,
            instance.id());

        Message request =
            composeRequest("heartbeat", new HeartbeatRequest(currentTerm.get().toBytes(), this.memberId));
        Address address = instance.address();

        cluster.listen().filter(m->m.correlationId().equals(request.correlationId()))
            .subscribe(next -> {
              HeartbeatResponse response = next.data();
              LogicalTimestamp term = LogicalTimestamp.fromBytes(response.term());
              if (currentTerm.get().isBefore(term)) {
                LOGGER.info("service: [{}] member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.",
                    serviceName, this.memberId,
                    currentTerm.get(), term);
                currentTerm.set(term);
              }
            });
        
        cluster.send(address, request).subscribe();
      });

    };
  }

  
  private void sendElectionCampaign() {
    Collection<Member> services = findPeers();
    CountDownLatch consensus = new CountDownLatch((services.size() / 2));

    services.stream()
        .forEach(instance -> {

          LOGGER.info("service: [{}] member: [{}] sending vote request to: [{}].", serviceName, this.memberId,
              instance.id());
         
          Message request = composeRequest("vote", new VoteRequest(currentTerm.get().toBytes(),
              instance.id()));

          requestOne(request, instance.address())
              .subscribe(next -> {
                VoteResponse vote = next.data();
                LOGGER.info("service: [{}] member: [{}] recived vote response: [{}].", serviceName, this.memberId,
                    vote);
                if (vote.granted()) {
                  consensus.countDown();
                }
              });
        });

    try {
      consensus.await(3, TimeUnit.SECONDS);
      stateMachine.transition(State.LEADER, currentTerm);
    } catch (InterruptedException e) {
      stateMachine.transition(State.FOLLOWER, currentTerm);
    }
  }

  private Mono<Message> requestOne(Message request, Address address) {
	return Mono.create(s->{
	  cluster.listen()
		.filter(m->m.correlationId().equals(request.correlationId()))
		.subscribe(m->{
			
		});
	 });
  }

/**
   * node becomes leader when most of the peers granted a vote on election process.
   * 
   * @return
   */
  private Consumer becomeLeader() {
    return leader -> {

      LOGGER.info("service: [{}] member: [{}] has become: [{}].", serviceName, this.memberId,
          stateMachine.currentState());
      
      timeoutScheduler.stop();
      heartbeatScheduler.start(config.heartbeatInterval());
      this.currentLeader.set(this.memberId);

      // spread the gossip about me as a new leader.
      this.cluster.spreadGossip(newLeaderElectionGossip(State.LEADER));
      CompletableFuture.runAsync(() -> onBecomeLeader());
    };
  }

  public abstract void onBecomeLeader();

  private Message newLeaderElectionGossip(State state) {
    return Message.builder()
        .header(LeaderElectionGossip.TYPE, state.toString())
        .data(new LeaderElectionGossip(this.memberId, this.currentLeader.get(), this.currentTerm.get().toLong(),
            this.cluster.address()))
        .build();
  }

  /**
   * node becomes candidate when no heartbeats received until timeout has reached. when at state candidate node is
   * ticking the term and sending vote requests to all peers. peers grant vote to candidate if peers current term is
   * older than node new term
   * 
   * @return
   */
  private Consumer becomeCandidate() {
    return election -> {
      LOGGER.info("service: [{}] member: [{}] has become: [{}].", serviceName, this.memberId,
          stateMachine.currentState());
      heartbeatScheduler.stop();
      currentTerm.set(clock.tick());
      sendElectionCampaign();

      // spread the gossip about me as candidate.
      this.cluster.spreadGossip(newLeaderElectionGossip(State.CANDIDATE));
      CompletableFuture.runAsync(() -> onBecomeCandidate());
    };
  }

  public abstract void onBecomeCandidate();

  /**
   * node became follower when it initiates
   * 
   * @return
   */
  private Consumer becomeFollower() {
    return follower -> {
      LOGGER.info("service: [{}] member: [{}] has become: [{}].", serviceName, this.memberId,
          stateMachine.currentState());
      heartbeatScheduler.stop();
      timeoutScheduler.start(this.timeout);

      // spread the gossip about me as follower.
      this.cluster.spreadGossip(newLeaderElectionGossip(State.FOLLOWER));
      CompletableFuture.runAsync(() -> onBecomeFollower());
    };
  }

  public abstract void onBecomeFollower();

  private Message composeRequest(String action, Object data) {
    return Message.builder()
    	.correlationId(IdGenerator.generateId())
        .qualifier("/"+ this.serviceName+ "/" + action)
        .data(data).build();
  }

  private Collection<Member> findPeers() {
    return cluster().otherMembers();
  }

  public void on(State state, Consumer func) {
    stateMachine.on(state, func);
  }

}
