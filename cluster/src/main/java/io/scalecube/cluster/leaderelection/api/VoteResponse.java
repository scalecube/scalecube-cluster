package io.scalecube.cluster.leaderelection.api;

public class VoteResponse {

  private boolean granted;
  private String memberId;

  public VoteResponse() {}

  public VoteResponse(boolean granted, String memberId) {
    this.granted = granted;
    this.memberId = memberId;
  }

  public boolean granted() {
    return this.granted;
  }

  public String memberId() {
    return memberId;
  }

  @Override
  public String toString() {
    return "VoteResponse [granted=" + granted + ", memberId=" + memberId + "]";
  }
}
