package io.scalecube.cluster.leaderelection;

import io.scalecube.cluster.leaderelection.api.HeartbeatRequest;
import io.scalecube.cluster.leaderelection.api.VoteRequest;
import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

public class Protocol {

  private static final String HEARTBEAT = "/heartbeat";

  private static final String VOTE = "/vote";

  public static Message asRequest(Address sender, String topic, String action, Object data) {
    return Message.builder().sender(sender).correlationId(IdGenerator.generateId())
        .qualifier(topic + action).data(data).build();
  }

  public static Message asHeartbeatRequest(Address sender, String topic, HeartbeatRequest data) {
    return asRequest(sender, topic, HEARTBEAT, data);
  }

  public static boolean isHeartbeat(String topic, String value) {
    return (topic + HEARTBEAT).equalsIgnoreCase(value);
  }

  public static Message asVoteRequest(Address sender, String topic, VoteRequest data) {
    return asRequest(sender, topic, VOTE, data);
  }

  public static boolean isVote(String topic, String value) {
    return (topic + VOTE).equalsIgnoreCase(value);
  }

}
