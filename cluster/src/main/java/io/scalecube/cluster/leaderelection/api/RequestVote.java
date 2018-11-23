package io.scalecube.cluster.leaderelection.api;

public class RequestVote {

  String candidateId;

  byte[] term;

  public String candidateId() {
    return candidateId;
  }

  public byte[] term() {
    return term;
  }

}
