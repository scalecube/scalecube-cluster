package io.scalecube.cluster;

import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.transport.Message;

public interface ClusterMessageHandler {

  default void onMembershipEvent(Message message) {}

  default void onGossip(Message gossip) {}

  default void onEvent(MembershipEvent event) {}
}
