package io.scalecube.cluster.membership;

import io.scalecube.net.Address;
import java.util.List;
import java.util.Map;

public interface MembershipConfig {

  List<Address> getSeedMembers();

  Map<String, String> getMetadata();

  int getSyncInterval();

  int getSyncTimeout();

  String getSyncGroup();

  int getPingInterval();

  int getSuspicionMult();

  String getMemberHost();

  Integer getMemberPort();
}
