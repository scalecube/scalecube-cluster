package io.scalecube.issues.i187;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.transport.Address;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeNoInboundRunner {

  public static final Logger logger = LoggerFactory.getLogger(NodeNoInboundRunner.class);

  public static final int PORT = 9090;
  public static final int SEED_PORT = 4545;

  /**
   * Main.
   *
   * @param args args
   * @throws Exception exceptoin
   */
  public static void main(String[] args) throws Exception {
    Address address = getSeedAddress(args).orElseGet(() -> Address.create("localhost", SEED_PORT));

    ClusterConfig config =
        ClusterConfig.builder()
            .syncGroup("issue187")
            .seedMembers(address)
            .addMetadata("node-i-th", Integer.toHexString(new Object().hashCode()))
            .syncInterval(1000)
            .syncTimeout(500)
            .gossipInterval(100)
            .gossipFanout(3)
            .gossipRepeatMult(3)
            .pingInterval(1000)
            .pingTimeout(500)
            .pingReqMembers(3)
            .metadataTimeout(1000)
            .connectTimeout(1000)
            .port(PORT)
            .build();

    logger.debug("Starting Node-With-No-Inbound-Traffic with config {}", config);
    Cluster cluster = Cluster.joinAwait(config);
    logger.debug(
        "Started Node-With-No-Inbound-Traffic: {}, address: {}", cluster, cluster.address());

    Thread.currentThread().join();
  }

  private static Optional<Address> getSeedAddress(String[] args) {
    if (args.length == 0) {
      return Optional.empty();
    }
    String addressArg = args[0];
    if (addressArg.isEmpty()) {
      return Optional.empty();
    }
    try {
      return Optional.of(Address.from(addressArg));
    } catch (Exception ex) {
      return Optional.empty();
    }
  }
}
