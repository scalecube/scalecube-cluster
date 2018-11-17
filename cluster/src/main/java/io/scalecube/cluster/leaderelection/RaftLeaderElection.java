package io.scalecube.cluster.leaderelection;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.leaderelection.api.HeartbeatRequest;
import io.scalecube.cluster.leaderelection.api.HeartbeatResponse;
import io.scalecube.cluster.leaderelection.api.Leader;
import io.scalecube.cluster.leaderelection.api.LeaderElectionGossip;
import io.scalecube.cluster.leaderelection.api.VoteRequest;
import io.scalecube.cluster.leaderelection.api.VoteResponse;
import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import reactor.core.publisher.Mono;

public abstract class RaftLeaderElection {

  private static final String LEADER_ELECTION_TOPIC = "leader-election-topic";

private static final Logger LOGGER = LoggerFactory.getLogger(RaftLeaderElection.class);

private static final String LEADER_ELECTION = "/sc.leader-election@";

  private final StateMachine stateMachine;

  private Cluster cluster;

  private final LogicalClock clock = new LogicalClock();

  private final AtomicReference<LogicalTimestamp> currentTerm = new AtomicReference<LogicalTimestamp>();

  private JobScheduler timeoutScheduler;

  private JobScheduler heartbeatScheduler;

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
  
  public RaftLeaderElection(String serviceName, Config config) {
    this.config = config;
    this.serviceName = serviceName;
    this.timeout = new Random().nextInt(config.timeout() - (config.timeout() / 2)) + (config.timeout() / 2);
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

  public void start(Cluster cluster) {
	
    this.memberId = cluster.member().id();
    this.cluster = cluster;
    this.stateMachine.transition(State.FOLLOWER, currentTerm.get());
    
    cluster.listen().filter(m->m.qualifier().equals(LEADER_ELECTION + serviceName+"/hearbeat"))
    	.subscribe(request -> {
    		onHeartbeat(request.data())
	    	.subscribe(resp -> {
	    		cluster.send(request.sender(),Message.from(request).withData(resp).build());
	    	});
    });
    
    cluster.listen().filter(m->m.qualifier().equals(LEADER_ELECTION + serviceName+"/vote"))
    	.subscribe(request->{
    		onRequestVote(request.data())
    		.subscribe(resp -> {
    			cluster.send(request.sender(),Message.from(request).withData(resp).build());
    		});
    });
    
    Map<String, String> metadata = new HashMap<>();
    metadata.putAll(cluster.member().metadata());
    metadata.put(serviceName, LEADER_ELECTION_TOPIC);
	cluster.updateMetadata(metadata);
    
  }

  public Mono<Leader> leader() {
    return Mono.just(new Leader(this.memberId, this.currentLeader.get()));
  }

  private Mono<HeartbeatResponse> onHeartbeat(HeartbeatRequest request) {
    LOGGER.debug("member [{}] recived heartbeat request: [{}]", this.memberId, request);
    this.timeoutScheduler.reset(this.timeout);
    
    LogicalTimestamp term = LogicalTimestamp.fromBytes(request.term());
    if (currentTerm.get().isBefore(term)) {
      LOGGER.info("member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.",
          this.memberId,
          currentTerm.get(), term);
      currentTerm.set(term);
    }

    if (!stateMachine.currentState().equals(State.FOLLOWER)) {

      LOGGER.info("member [{}] currentState [{}] and recived heartbeat. becoming FOLLOWER.",
          this.memberId,
          stateMachine.currentState());
      stateMachine.transition(State.FOLLOWER, term);
    }
    

    this.currentLeader.set(request.memberId());

    return Mono.just(new HeartbeatResponse(this.memberId, currentTerm.get().toBytes()));
  }


  private Mono<VoteResponse> onRequestVote(VoteRequest request) {

    LogicalTimestamp term = LogicalTimestamp.fromBytes(request.term());

    boolean voteGranted = currentTerm.get().isBefore(term);
    LOGGER.info("member [{}:{}] recived vote request: [{}] voteGranted: [{}].",
        this.memberId,
        stateMachine.currentState(), request, voteGranted);

    if (currentTerm.get().isBefore(term)) {
      LOGGER.info("member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.",
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
      LOGGER.info("member: [{}] didnt recive heartbeat until timeout: [{}ms] became: [{}]",
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
        LOGGER.debug("member: [{}] sending heartbeat: [{}].", this.memberId,
            instance.id());

        Message request =
            asRequest("heartbeat", new HeartbeatRequest(currentTerm.get().toBytes(), this.memberId));
        Address address = instance.address();

        cluster.listen().filter(m->m.correlationId().equals(request.correlationId()))
            .subscribe(next -> {
              HeartbeatResponse response = next.data();
              LogicalTimestamp term = LogicalTimestamp.fromBytes(response.term());
              if (currentTerm.get().isBefore(term)) {
                LOGGER.info("member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.",
                    this.memberId,
                    currentTerm.get(), term);
                currentTerm.set(term);
              }
            });
        
        cluster.send(address, request).subscribe();
      });

    };
  }

  // election requesting votes from peers and try reach consensus.
  // if most of peer members grant vote then member transition to a leader state.
  // the election process is waiting until timeout of consensusTimeout in case timeout reached the member transition to Follower state.
  private void sendElectionCampaign() {
    Collection<Member> members = findPeers();
    
    CountDownLatch consensus = new CountDownLatch((members.size() / 2));

    members.stream()
        .forEach(instance -> {

          LOGGER.info("member: [{}] sending vote request to: [{}].", this.memberId,
              instance.id());
         
          Message request = asRequest("vote", new VoteRequest(currentTerm.get().toBytes(),
              instance.id()));

          requestOne(request, instance.address())
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

      LOGGER.info("member: [{}] has become: [{}].", this.memberId,
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
      LOGGER.info("member: [{}] has become: [{}].", this.memberId,
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
      LOGGER.info("member: [{}] has become: [{}].", this.memberId,
          stateMachine.currentState());
      heartbeatScheduler.stop();
      timeoutScheduler.start(this.timeout);

      // spread the gossip about me as follower.
      this.cluster.spreadGossip(newLeaderElectionGossip(State.FOLLOWER));
      CompletableFuture.runAsync(() -> onBecomeFollower());
    };
  }

  public abstract void onBecomeFollower();

  private Message asRequest(String action, Object data) {
    return Message.builder()
    	.correlationId(IdGenerator.generateId())
        .qualifier(LEADER_ELECTION + serviceName+ "/" + action)
        .data(data)
        .build();
  }

  private Collection<Member> findPeers() {
    return cluster().otherMembers().stream()
    		.filter(m-> m.metadata().containsKey(serviceName))
    		.collect(Collectors.toSet());
    		
  }

  private void on(State state, Consumer func) {
    stateMachine.on(state, func);
  }

}
