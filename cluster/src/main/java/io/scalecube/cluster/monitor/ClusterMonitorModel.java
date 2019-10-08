package io.scalecube.cluster.monitor;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.net.Address;
import java.util.List;
import java.util.function.Supplier;

public class ClusterMonitorModel {

  private Cluster cluster;
  private ClusterConfig config;
  private Supplier<Integer> incarnationSupplier;
  private List<Address> seedMembers;
  private Supplier<List<Member>> aliveMembersSupplier;
  private Supplier<List<Member>> suspectedMembersSupplier;
  private Supplier<List<Member>> removedMembersSupplier;

  private ClusterMonitorModel(Builder builder) {
    this.cluster = builder.cluster;
    this.config = builder.config;
    this.incarnationSupplier = builder.incarnationSupplier;
    this.seedMembers = builder.seedMembers;
    this.aliveMembersSupplier = builder.aliveMembersSupplier;
    this.suspectedMembersSupplier = builder.suspectedMembersSupplier;
    this.removedMembersSupplier = builder.removedMembersSupplier;
  }

  public ClusterConfig config() {
    return config;
  }

  public int incarnation() {
    return incarnationSupplier.get();
  }

  public int clusterSize() {
    return cluster.otherMembers().size() + 1;
  }

  public Member member() {
    return cluster.member();
  }

  public Object metadata() {
    return cluster.metadata().orElse(null);
  }

  public List<Address> seedMembers() {
    return seedMembers;
  }

  public List<Member> aliveMembers() {
    return aliveMembersSupplier.get();
  }

  public List<Member> suspectedMembers() {
    return suspectedMembersSupplier.get();
  }

  public List<Member> removedMembers() {
    return removedMembersSupplier.get();
  }

  public static class Builder {

    private Cluster cluster;
    private ClusterConfig config;
    private Supplier<Integer> incarnationSupplier;
    private List<Address> seedMembers;
    private Supplier<List<Member>> aliveMembersSupplier;
    private Supplier<List<Member>> suspectedMembersSupplier;
    private Supplier<List<Member>> removedMembersSupplier;

    public ClusterMonitorModel build() {
      return new ClusterMonitorModel(this);
    }

    public Builder config(ClusterConfig config) {
      this.config = config;
      return this;
    }

    public Builder cluster(Cluster cluster) {
      this.cluster = cluster;
      return this;
    }

    public Builder incarnationSupplier(Supplier<Integer> incarnationSupplier) {
      this.incarnationSupplier = incarnationSupplier;
      return this;
    }

    public Builder seedMembers(List<Address> seedMembers) {
      this.seedMembers = seedMembers;
      return this;
    }

    public Builder aliveMembersSupplier(Supplier<List<Member>> aliveMembersSupplier) {
      this.aliveMembersSupplier = aliveMembersSupplier;
      return this;
    }

    public Builder suspectedMembersSupplier(Supplier<List<Member>> suspectedMembersSupplier) {
      this.suspectedMembersSupplier = suspectedMembersSupplier;
      return this;
    }

    public Builder removedMembersSupplier(Supplier<List<Member>> removedMembersSupplier) {
      this.removedMembersSupplier = removedMembersSupplier;
      return this;
    }
  }
}
