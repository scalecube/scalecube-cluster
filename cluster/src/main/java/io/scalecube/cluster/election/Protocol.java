package io.scalecube.cluster.election;

import io.scalecube.cluster.election.api.HeartbeatRequest;
import io.scalecube.cluster.election.api.VoteRequest;
import io.scalecube.cluster.election.api.VoteResponse;
import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import reactor.core.publisher.Mono;

public class Protocol {

  private static final String HEARTBEAT = "/heartbeat";

  private static final String VOTE = "/vote";

  public static Mono<Message> FALSE_VOTE =
      Mono.just(new VoteResponse(false, null)).map(Message::fromData);

  /**
   * create request message in the context of a given election topic.
   *
   * @param sender of this request.
   * @param topic of this election.
   * @param action of the request.
   * @param data to be sent.
   * @return request message.
   */
  public static Message asRequest(Address sender, String topic, String action, Object data) {
    return Message.builder()
        .sender(sender)
        .correlationId(IdGenerator.generateId())
        .qualifier(topic + action)
        .data(data)
        .build();
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
