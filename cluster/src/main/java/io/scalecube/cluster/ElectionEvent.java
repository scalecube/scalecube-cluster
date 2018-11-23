package io.scalecube.cluster;

import io.scalecube.cluster.leaderelection.State;

public class ElectionEvent {

  private State state;

  public ElectionEvent(State state) {
    this.state = state;
  }

  public State state() {
    return state;
  }
}
