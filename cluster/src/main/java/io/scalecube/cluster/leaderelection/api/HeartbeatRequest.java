package io.scalecube.cluster.leaderelection.api;

import java.util.Arrays;

public class HeartbeatRequest {

  private byte[] term;
  private String memberId;

  public HeartbeatRequest() {};


  public HeartbeatRequest(byte[] term, String memberId) {
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
    return "HeartbeatRequest [term=" + Arrays.toString(term) + ", memberId=" + memberId + "]";
  }

}

