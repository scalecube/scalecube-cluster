package io.scalecube.cluster;

import io.scalecube.cluster.membership.MembershipEvent;

@FunctionalInterface
public interface ClusterEventHandler extends ClusterMessageHandler {

  @Override
  void onEvent(MembershipEvent event);
}
