package io.scalecube.cluster.leaderelection.api;

public class HeartbeatResponse {

  private long term;

  private String memberId;

  public HeartbeatResponse() {}

  public HeartbeatResponse(String memberId, long term) {
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
    return "HeartbeatResponse [term=" + term + ", memberId=" + memberId + "]";
  }
}
