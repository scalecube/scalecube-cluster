package io.scalecube.cluster.membership;

import io.scalecube.net.Address;
import java.util.List;

public interface MembershipConfig {

  List<Address> getSeedMembers();

  int getSyncInterval();

  int getSyncTimeout();

  String getSyncGroup();

  int getPingInterval();

  int getSuspicionMult();

  String getMemberHost();

  Integer getMemberPort();
}
