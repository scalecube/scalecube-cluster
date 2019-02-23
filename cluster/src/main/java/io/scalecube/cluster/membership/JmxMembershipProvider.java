package io.scalecube.cluster.membership;


import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class JmxMembershipProvider {
  private final MembershipProtocolImpl membership;
  private final Cluster cluster;

  public JmxMembershipProvider(Cluster cluster,
                               MembershipProtocolImpl membership) {
    this.cluster = cluster;
    this.membership = membership;

  }


  public int incarnation() {
    Member current = cluster.member();
    return membership.getMembershipRecords()
                        .stream()
                        .filter( rec -> rec.id().equals(current.id()) )
                        .map( MembershipRecord::incarnation )
                        .findFirst()
                        .orElse(-1);
  }

  public List<String> aliveMembers() {
    return findRecordsByCondition(MembershipRecord::isAlive);
  }

  public List<String> deadMembers() {
    return findRecordsByCondition( MembershipRecord::isDead );
  }

  public List<String> suspectedMembers() {
    return findRecordsByCondition(MembershipRecord::isSuspect);
  }

  private List<String> findRecordsByCondition(Predicate<MembershipRecord> condition){
    return membership.getMembershipRecords()
      .stream()
      .filter(condition)
      .map( record -> new Member( record.id() , record.address())  )
      .map( Member::toString)
      .collect(Collectors.toList());
  }

}
