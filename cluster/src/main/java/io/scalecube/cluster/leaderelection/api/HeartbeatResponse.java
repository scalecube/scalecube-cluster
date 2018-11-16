package io.scalecube.cluster.leaderelection.api;

import java.util.Arrays;

public class HeartbeatResponse {

  private byte[] term;

  private String memberId;

  public HeartbeatResponse() {};


  public HeartbeatResponse(String memberId, byte[] term) {
    this.term = term;
    this.memberId = memberId;
  }

  public byte[] term() {
    return term;
  }

  public String memberId() {
    return memberId;
  }

  @Override
  public String toString() {
    return "HeartbeatResponse [term=" + Arrays.toString(term) + ", memberId=" + memberId + "]";
  }

}
