package io.scalecube.cluster.leaderelection.api;

public class HeartbeatRequest {

  private long term;
  private String memberId;

  public HeartbeatRequest() {};

  public HeartbeatRequest(long term, String memberId) {
    this.term = term;
    this.memberId = memberId;
  }

  public long term() {
    return term;
  }

  public String memberId() {
    return memberId;
  }

  @Override
  public String toString() {
    return "HeartbeatRequest [term=" + term + ", memberId=" + memberId + "]";
  }
}
