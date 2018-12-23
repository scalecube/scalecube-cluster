package io.scalecube.cluster.election.api;

public class ElectionEvent {

  private static ElectionEvent follower = new ElectionEvent(State.FOLLOWER);
  private static ElectionEvent candidate = new ElectionEvent(State.CANDIDATE);
  private static ElectionEvent leader = new ElectionEvent(State.LEADER);
  private State state;

  public ElectionEvent(State state) {
    this.state = state;
  }

  public State state() {
    return state;
  }

  public static ElectionEvent follower() {
    return follower;
  }

  public static ElectionEvent candidate() {
    return candidate;
  }

  public static ElectionEvent leader() {
    return leader;
  }
}
