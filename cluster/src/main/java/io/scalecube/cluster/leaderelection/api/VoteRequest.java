package io.scalecube.cluster.leaderelection.api;

public class VoteRequest {

  private long term;

  public VoteRequest() {};

  public VoteRequest(long term) {
    this.term = term;
  }

  public long term() {
    return term;
  }
}
