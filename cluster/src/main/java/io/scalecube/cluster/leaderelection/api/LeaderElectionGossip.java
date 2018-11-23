package io.scalecube.cluster.leaderelection.api;


import io.scalecube.transport.Address;

public class LeaderElectionGossip {

  public static final String TYPE = "sc-leader-election";

  private String memberId;
  private long term;
  private Address address;
  private String leaderId;

  public LeaderElectionGossip(String memberId, String leaderId, long term, Address address) {
    this.memberId = memberId;
    this.leaderId = leaderId;
    this.term = term;
    this.address = address;
  }

  public String leaderId() {
    return leaderId;
  }

  public String memberId() {
    return this.memberId;
  }

  public long term() {
    return this.term;
  }

  public Address address() {
    return this.address;
  }

  public boolean isLeader() {
    return this.memberId.equals(this.leaderId);
  }
}
