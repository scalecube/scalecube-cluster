package io.scalecube.cluster.leaderelection;

public enum State {

  /**
   * Represents the state of an inactive server.
   * <p>
   * All servers start in this state and return to this state when {@link #leave() stopped}.
   */
  INACTIVE,

  /**
   * Represents the state of a server participating in normal log replication.
   * <p>
   * The follower state is a standard Raft state in which the server receives replicated log entries from the leader.
   */
  FOLLOWER,

  /**
   * Represents the state of a server attempting to become the leader.
   * <p>
   * When a server in the follower state fails to receive communication from a valid leader for some time period,
   * the follower will transition to the candidate state. During this period, the candidate requests votes from
   * each of the other servers in the cluster. If the candidate wins the election by receiving votes from a majority
   * of the cluster, it will transition to the leader state.
   */
  CANDIDATE,

  /**
   * Represents the state of a server which is actively coordinating and replicating logs with other servers.
   * <p>
   * Leaders are responsible for handling and replicating writes from clients. Note that more than one leader can
   * exist at any given time, but Raft guarantees that no two leaders will exist for the same {@link Cluster#term()}.
   */
  LEADER

}
