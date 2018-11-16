package io.scalecube.cluster.leaderelection.api;


import reactor.core.publisher.Mono;

public interface LeaderElectionService {

  
  public Mono<Leader> leader();

  
  Mono<HeartbeatResponse> onHeartbeat(HeartbeatRequest request);
  
  
  Mono<VoteResponse> onRequestVote(VoteRequest request);
  
}
