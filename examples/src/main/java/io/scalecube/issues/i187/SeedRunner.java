package io.scalecube.issues.i187;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeedRunner {

  public static final Logger logger = LoggerFactory.getLogger(SeedRunner.class);

  public static final int DEFALT_PORT = 4545;

  /**
   * Maibn.
   *
   * @param args args
   * @throws Exception exception
   */
  public static void main(String[] args) throws Exception {
    int port = getPort(args).orElse(DEFALT_PORT);

    ClusterConfig config =
        ClusterConfig.builder()
            .syncGroup("issue187")
            .addMetadata("seed", Integer.toHexString(new Object().hashCode()))
            .syncInterval(1000)
            .syncTimeout(1000)
            .membershipPingTimeout(1000)
            .metadataTimeout(1000)
            .connectTimeout(1000)
            .port(port)
            .build();

    logger.debug("Starting Seed with config {}", config);
    Cluster cluster = Cluster.joinAwait(config);
    logger.debug("Started Seed: {}, address: {}", cluster, cluster.address());

    Thread.currentThread().join();
  }

  private static Optional<Integer> getPort(String[] args) {
    if (args.length < 1) {
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
}
