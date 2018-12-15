package io.scalecube.cluster.leaderelection.api;

public class RequestVote {

  private long term;
  
  public long term() {
    return term;
  }

}
