package io.scalecube.cluster.election;

import io.scalecube.cluster.election.api.State;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftStateMachine {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElectionProtocol.class);

  private final String id;

  private final Term term = new Term();

  private final AtomicReference<String> currentLeader = new AtomicReference<String>("");

  private final StateMachine stateMachine;

  private final JobScheduler timeoutScheduler;
  private final JobScheduler heartbeatScheduler;
  
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

  private RaftStateMachine(Builder builder) {
    this.id = builder.id;
    this.timeout =
        new Random().nextInt(builder.timeout - (builder.timeout / 2)) + (builder.timeout / 2);
    this.heartbeatInterval = builder.heartbeatInterval;
    this.stateMachine =
        StateMachine.builder()
            .init(State.INACTIVE)
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

  /**
   * function to execute on follower state.
   *
   * @param func consumer to be executed.
   */
  public void onFollower(Consumer func) {
    Consumer action =
        act -> {
          heartbeatScheduler.stop();
          timeoutScheduler.start(this.timeout);
        };
    stateMachine.on(State.FOLLOWER, action.andThen(func).andThen(l -> logStateChanged()));
  }

  /**
   * function to execute on candidate state.
   *
   * @param func consumer to be executed.
   */
  public void onCandidate(Consumer func) {
    Consumer action =
        act -> {
          heartbeatScheduler.stop();
          timeoutScheduler.stop();
        };
    stateMachine.on(State.CANDIDATE, action.andThen(func).andThen(l -> logStateChanged()));
  }

  /**
   * function to execute on leader state.
   *
   * @param func consumer to be executed.
   */
  public void onLeader(Consumer func) {
    Consumer action =
        act -> {
          heartbeatScheduler.start(this.heartbeatInterval);
        };

    stateMachine.on(State.LEADER, action.andThen(func).andThen(l -> logStateChanged()));
  }

  /** Transition to state follower. */
  public void becomeFollower() {
    this.stateMachine.transition(State.FOLLOWER);
  }

  /** Transition to state candidate. */
  public void becomeCandidate() {
    this.stateMachine.transition(State.CANDIDATE);
  }

  /** Transition to state leader. */
  public void becomeLeader() {
    this.currentLeader.set(this.id);
    this.stateMachine.transition(State.LEADER);
  }

  /**
   * handle heartbeat. only leaders are sending heartbeats when heartbeat arrives it reset the
   * current member timer expecting next heartbeat. in case not in state follower and heartbeat
   * arrives then transition to state follower.
   *
   * @param memberId of the leader sending the hearbeat.
   * @param term of the leader sending this heartbeat.
   */
  public void heartbeat(String memberId, long term) {
    this.timeoutScheduler.reset(this.timeout);
    if (this.term.isBefore(term)) {
      LOGGER.info(
          "[{},{},{}] term is before: [{}] setting new seen term.",
          this.id,
          this.stateMachine.currentState(),
          this.term.getLong(),
          term);
      this.term.set(term);
    }

    if (!isFollower()) {

      LOGGER.info(
          "[{},{},{}] and recived heartbeat. becoming FOLLOWER.",
          this.id,
          this.stateMachine.currentState(),
          this.term.getLong());
      becomeFollower();
    } else {
      this.currentLeader.set(memberId);
    }
  }

  /**
   * increment the next term.
   *
   * @return next term number.
   */
  public long nextTerm() {
    return this.term.nextTerm();
  }

  /**
   * get the current term.
   *
   * @return current term.
   */
  public Term currentTerm() {
    return this.term;
  }

  /**
   * update term value provided is after this term.
   *
   * @param value to evaluate as after.
   */
  public void updateTerm(long value) {
    if (term.isBefore(value)) {
      LOGGER.info(
          "[{},{},{}] term is before: [{}] setting new seen term.",
          this.id,
          this.stateMachine.currentState(),
          this.term.getLong(),
          value);
      term.set(value);
    }
  }

  private Consumer onHeartbeatNotRecived() {
    return toCandidate -> {
      LOGGER.info(
          "[{},{},{}] didnt recive heartbeat until timeout: [{}ms] became: [CANDIDATE]",
          this.id,
          this.stateMachine.currentState(),
          this.term.getLong(),
          this.timeout);
      becomeCandidate();
    };
  }

  private void logStateChanged() {
    LOGGER.info(
        "[{},{},{}] State changed.",
        this.id,
        this.stateMachine.currentState(),
        this.term.getLong(),
        term.getLong());
  }
}
