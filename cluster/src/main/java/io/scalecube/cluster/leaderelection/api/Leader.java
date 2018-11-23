package io.scalecube.cluster.leaderelection.api;

public class Leader {

  private String memberId;
  private String leaderId;

  public Leader() {};

  public Leader(String memberId, String leaderId) {
    this.memberId = memberId;
    this.leaderId = leaderId;
  }

  public String memberId() {
    return memberId;
  }

  public String leaderId() {
    return leaderId;
  }

  @Override
  public String toString() {
    return "Leader [memberId=" + memberId + ", leaderId=" + leaderId + "]";
  }
}
