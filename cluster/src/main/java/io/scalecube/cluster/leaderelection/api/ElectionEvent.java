package io.scalecube.cluster.leaderelection.api;

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
