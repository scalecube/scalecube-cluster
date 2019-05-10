package io.scalecube.issues.i187;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.transport.Address;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeNoInboundRunner {

  public static final Logger logger = LoggerFactory.getLogger(NodeNoInboundRunner.class);

  public static final int DEFAULT_PORT = 9090;
  public static final int DEFAULT_SEED_PORT = 4545;

  /**
   * Main.
   *
   * @param args args
   * @throws Exception exceptoin
   */
  public static void main(String[] args) throws Exception {
    Address address =
        getSeedAddress(args).orElseGet(() -> Address.create("localhost", DEFAULT_SEED_PORT));
    int port = getPort(args).orElse(DEFAULT_PORT);

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
            .port(port)
            .build();

    logger.debug("Starting Node-With-No-Inbound with config {}", config);
    Cluster cluster = Cluster.joinAwait(config);
    logger.debug("Started Node-With-No-Inbound: {}, address: {}", cluster, cluster.address());

    Thread.currentThread().join();
  }

  private static Optional<Integer> getPort(String[] args) {
    if (args.length < 2) {
      return Optional.empty();
    }
    String portArg = args[0];
    if (portArg.isEmpty()) {
      return Optional.empty();
    }
    try {
      return Optional.of(Integer.parseInt(portArg));
    } catch (Exception ex) {
      logger.error("Error in getPort: " + ex);
      return Optional.empty();
    }
  }

  private static Optional<Address> getSeedAddress(String[] args) {
    if (args.length < 2) {
      return Optional.empty();
    }
    String addressArg = args[1];
    if (addressArg.isEmpty()) {
      return Optional.empty();
    }
    try {
      return Optional.of(Address.from(addressArg));
    } catch (Exception ex) {
      logger.error("Error in getSeedAddress: " + ex);
      return Optional.empty();
    }
  }
}
