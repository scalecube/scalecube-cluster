package io.scalecube.cluster;

import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;

public interface ClusterMessageHandler {

  default void onMessage(Message message) {
    // no-op
  }

  default void onGossip(Message gossip) {
    // no-op
  }

  default void onMembershipEvent(MembershipEvent event) {
    // no-op
  }
}
