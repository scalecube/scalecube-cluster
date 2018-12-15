package io.scalecube.cluster.leaderelection;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftStateMachine {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftLeaderElection.class);

  private final String id;

  private final StateMachine stateMachine;

  private final JobScheduler timeoutScheduler;

  private final JobScheduler heartbeatScheduler;

  private final AtomicReference<String> currentLeader = new AtomicReference<String>("");

  private final Term term = new Term();
  
  private final int timeout;
  private final int heartbeatInterval;
  
  public boolean isFollower() {
    return this.currentState().equals(State.FOLLOWER);
  }

  public boolean isCandidate() {
    return this.currentState().equals(State.CANDIDATE);
  }

  public boolean isLeader() {
    return this.currentState().equals(State.LEADER);
  }
  
  public String leaderId() {
    return currentLeader.get();
  }
  
  public State currentState() {
    return this.stateMachine.currentState();
  }

  
  public static class Builder {

    private String id;
    private int timeout;
    private int heartbeatInterval;
    private Consumer sendHeartbeat;

    public Builder id(String id) {
      this.id = id;
      return this;
    }

    public Builder timeout(int timeout) {
      this.timeout = timeout;
      return this;
    }

    public Builder heartbeatInterval(int heartbeatInterval) {
      this.heartbeatInterval = heartbeatInterval;
      return this;
    }

    public Builder heartbeatSender(Consumer sendHeartbeat) {
      this.sendHeartbeat = sendHeartbeat;
      return this;
    }

    public RaftStateMachine build() {
      return new RaftStateMachine(this);
    }

  }

  public static Builder builder() {
    return new Builder();
  }

  public RaftStateMachine(Builder builder) {
    this.id = builder.id;
    this.timeout = builder.timeout;
    this.heartbeatInterval = builder.heartbeatInterval;
    this.stateMachine = StateMachine.builder().init(State.INACTIVE)
        .addTransition(State.INACTIVE, State.FOLLOWER)
        .addTransition(State.FOLLOWER, State.CANDIDATE)
        .addTransition(State.CANDIDATE, State.LEADER)
        .addTransition(State.CANDIDATE, State.FOLLOWER)
        .addTransition(State.LEADER, State.FOLLOWER)
        .build();

    this.timeoutScheduler = new JobScheduler(onHeartbeatNotRecived());
    this.heartbeatScheduler = new JobScheduler(builder.sendHeartbeat);
    term.nextTerm();

  }

  public void onFollower(Consumer func) {
    Consumer action =  act ->  {
      heartbeatScheduler.stop();
      timeoutScheduler.start(this.timeout);
    };
    stateMachine.on(State.FOLLOWER, action.andThen(func)
        .andThen(l->logStateChanged()));
    
  }
  public void onCandidate(Consumer func) {
    Consumer action =  act ->  {
      heartbeatScheduler.stop();
      timeoutScheduler.stop();
    };
    stateMachine.on(State.CANDIDATE, action.andThen(func)
        .andThen(l->logStateChanged()));
  }

  public void onLeader(Consumer func) {
    Consumer action =  act ->  {
      heartbeatScheduler.start(this.heartbeatInterval);
    };
    
    stateMachine.on(State.LEADER, action.andThen(func)
        .andThen(l->logStateChanged()));
  }

  public void becomeFollower(long term) {
    this.stateMachine.transition(State.FOLLOWER, term);
  }

  public void becomeCandidate(long term) {
    this.stateMachine.transition(State.CANDIDATE, term);
  }

  public void becomeLeader(long term) {
    this.currentLeader.set(this.id);
    this.stateMachine.transition(State.LEADER, term);
  }

  public void heartbeat(String memberId, long term) {
    this.timeoutScheduler.reset(this.timeout);
    if (this.term.isBefore(term)) {
      LOGGER.info("member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.", this.id,
          this.term.getLong(), term);
      this.term.set(term);
    }

    if (!isFollower()) {

      LOGGER.info("member [{}] currentState [{}] and recived heartbeat. becoming FOLLOWER.",
          this.id, stateMachine.currentState());
      becomeFollower(term);
    } else {
      this.currentLeader.set(memberId);
    }
  }
  
  private Consumer onHeartbeatNotRecived() {
    return toCandidate -> {
      LOGGER.info("member: [{}] didnt recive heartbeat until timeout: [{}ms] became: [{}]", this.id,
          timeout, stateMachine.currentState());
      becomeCandidate(term.nextTerm());
    };
  }

  public long nextTerm() {
    return this.term.nextTerm();
  }
  
  public Term currentTerm() {
    return this.term;
  }

  public void updateTerm(long timestamp) {
    if (term.isBefore(timestamp)) {
      LOGGER.info("member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.", this.id,
          currentTerm(), timestamp);
      term.set(timestamp);
    }
  }
  
  private void logStateChanged() {
    LOGGER.info("member: [{}] has become: [{}] term: [{}].", this.id, stateMachine.currentState(), term.getLong());
  }
}
