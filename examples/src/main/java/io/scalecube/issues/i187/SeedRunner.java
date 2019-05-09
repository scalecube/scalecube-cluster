package io.scalecube.issues.i187;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeedRunner {

  public static final Logger logger = LoggerFactory.getLogger(SeedRunner.class);

  public static final int PORT = 4545;

  /**
   * Maibn.
   *
   * @param args args
   * @throws Exception exception
   */
  public static void main(String[] args) throws Exception {
    ClusterConfig config =
        ClusterConfig.builder()
            .syncGroup("issue187")
            .addMetadata("seed", Integer.toHexString(new Object().hashCode()))
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

    logger.debug("Starting Seed with config {}", config);
    Cluster cluster = Cluster.joinAwait(config);
    logger.debug("Started Seed: {}, address: {}", cluster, cluster.address());

    Thread.currentThread().join();
  }
}
