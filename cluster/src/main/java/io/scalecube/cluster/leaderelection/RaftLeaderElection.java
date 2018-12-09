package io.scalecube.cluster.leaderelection;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.leaderelection.api.HeartbeatRequest;
import io.scalecube.cluster.leaderelection.api.HeartbeatResponse;
import io.scalecube.cluster.leaderelection.api.Leader;
import io.scalecube.cluster.leaderelection.api.ElectionEvent;
import io.scalecube.cluster.leaderelection.api.ElectionTopic;
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
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RaftLeaderElection implements ElectionTopic {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftLeaderElection.class);

  private static final String LEADER_ELECTION = "leader-election";

  private final RaftStateMachine stateMachine;

  private String memberId;
  
  private final Config config;

  private final DirectProcessor<ElectionEvent> processor = DirectProcessor.create();

  private String topic;

  private Cluster cluster;

  @Override
  public String id() {
    return this.cluster.member().id();
  }

  @Override
  public String name() {
    return this.topic;
  }

  public State currentState() {
    return stateMachine.currentState();
  }

  public Mono<Leader> leader() {
    return Mono.just(new Leader(this.memberId, stateMachine.leaderId()));
  }

  protected Cluster cluster() {
    return this.cluster;
  }

  public RaftLeaderElection(Cluster cluser, String topic, Config config) {
    this.config = config;
    this.topic = topic;
    this.cluster = cluser;
    int timeout =
        new Random().nextInt(config.timeout() - (config.timeout() / 2)) + (config.timeout() / 2);

    this.stateMachine = RaftStateMachine.builder()
        .id(this.cluster.member().id())
        .timeout(timeout)
        .heartbeatInterval(config.heartbeatInterval())
        .heartbeatSender(sendHeartbeat())
        .build();
    
    this.stateMachine.onFollower(asFollower -> {
      
      // spread the gossip about me as follower.
      this.cluster.spreadGossip(newLeaderElectionGossip(State.FOLLOWER)).subscribe();
      this.processor.onNext(new ElectionEvent(State.FOLLOWER));

    });

    this.stateMachine.onCandidate(asCandidate -> {
      
      this.stateMachine.nextTerm();
      startElection();
      // spread the gossip about me as candidate.
      this.cluster.spreadGossip(newLeaderElectionGossip(State.CANDIDATE));
      this.processor.onNext(new ElectionEvent(State.CANDIDATE));
    });

    this.stateMachine.onLeader(asLeader -> {
      // spread the gossip about me as a new leader.
      this.cluster.spreadGossip(newLeaderElectionGossip(State.LEADER)).subscribe();
      this.processor.onNext(new ElectionEvent(State.LEADER));
    });
    
    this.stateMachine.becomeFollower(this.stateMachine.nextTerm());
  }

  @Override
  public Flux<ElectionEvent> listen() {
    return processor;
  }

  public RaftLeaderElection(Cluster cluster, String name) {
    this(cluster, name, Config.build());
    this.cluster = cluster;
    this.cluster.updateMetadataProperty(name, LEADER_ELECTION).subscribe();
  }

  public Mono<Void> start() {
    return Mono.create(sink -> {

      cluster.listen().filter(m -> RaftProtocol.isHeartbeat(topic, m.qualifier()))
          .subscribe(request -> {
            onHeartbeatRecived(request.data()).subscribe(resp -> {
              cluster
                  .send(request.sender(),
                      Message.from(request).withData(resp).sender(this.cluster.address()).build())
                  .subscribe();
            });
          });

      cluster.listen().filter(m -> RaftProtocol.isVote(topic, m.qualifier())).subscribe(request -> {
        onVoteRequested(request).subscribe(resp -> {
          cluster.send(request.sender(), resp).subscribe();
        });
      });

      this.memberId = cluster.member().id();
      this.stateMachine.becomeFollower(this.stateMachine.currentTerm());
    });
  }

  private Mono<HeartbeatResponse> onHeartbeatRecived(HeartbeatRequest request) {
    return Mono.create(sink -> {
      LOGGER.debug("member [{}] recived heartbeat request: [{}]", this.memberId, request);
      stateMachine.heartbeat(request.memberId(),LogicalTimestamp.fromLong(request.term()));
      sink.success(new HeartbeatResponse(this.memberId, stateMachine.currentTerm().toLong()));
    });
  }

  private Mono<Message> onVoteRequested(Message request) {
    VoteRequest voteReq = request.data();

    boolean voteGranted = stateMachine.currentTerm().isBefore(voteReq.term());
    LOGGER.info("member [{}:{}] recived vote request: [{}] voteGranted: [{}].", this.memberId,
        stateMachine.currentState(), request.data(), voteGranted);

    stateMachine.updateTerm(voteReq.term());

    return Mono.just(
        Message.builder().sender(this.cluster.address()).correlationId(request.correlationId())
            .data(new VoteResponse(voteGranted, this.memberId)).build());
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

        Message request = RaftProtocol.asHeartbeatRequest(this.cluster.address(), topic,
            new HeartbeatRequest(stateMachine.currentTerm().toLong(), this.memberId));

        requestOne(request, instance.address()).subscribe(next -> {
          HeartbeatResponse response = next.data();
          stateMachine.updateTerm(LogicalTimestamp.fromLong(response.term()));
        });
      });
    };
  }

  // election requesting votes from peers and try reach consensus.
  // if most of peer members grant vote then member transition to a leader state.
  // the election process is waiting until timeout of consensusTimeout in case
  // timeout reached the member transition to Follower state.
  private void startElection() {
    final Collection<Member> members = findPeers();
    final CountDownLatch consensus = new CountDownLatch((members.size() / 2));

    members.stream().forEach(instance -> {

      LOGGER.info("member: [{}] sending vote request to: [{}].", this.memberId, instance.id());

      Message request = RaftProtocol.asVoteRequest(this.cluster.address(), topic,
          new VoteRequest(stateMachine.currentTerm().toLong()));

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
      stateMachine.becomeLeader(stateMachine.currentTerm());
    } catch (InterruptedException e) {
      stateMachine.becomeFollower(stateMachine.currentTerm());
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
        .data(new LeaderElectionGossip(this.memberId, 
            this.stateMachine.leaderId(),
            this.stateMachine.currentTerm().toLong(), 
            this.cluster.address()))
        .build();
  }

  private Collection<Member> findPeers() {
    return cluster().otherMembers().stream().filter(m -> m.metadata().containsKey(topic))
        .filter(m -> m.metadata().get(topic).equals(LEADER_ELECTION)).collect(Collectors.toSet());

  }
}
