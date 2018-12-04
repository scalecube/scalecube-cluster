package io.scalecube.cluster.leaderelection.api;

public class RequestVote {

  long term;
  
  public long term() {
    return term;
  }

}
