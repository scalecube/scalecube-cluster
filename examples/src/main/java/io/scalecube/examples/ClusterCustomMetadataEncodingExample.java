package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.metadata.MetadataDecoder;
import io.scalecube.cluster.metadata.MetadataEncoder;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class ClusterCustomMetadataEncodingExample {

  /** Main method. */
  public static void main(String[] args) throws Exception {
    // Start seed cluster member Alice
    Cluster alice =
        new ClusterImpl()
            .config(config -> config.metadataDecoder(new LongMetadataDecoder()))
            .startAwait();
    System.out.println(
        "[" + alice.member().id() + "] Alice's metadata: " + alice.metadata().orElse(null));

    Cluster joe =
        new ClusterImpl()
            .config(
                config ->
                    config
                        .membership(opts -> opts.seedMembers(alice.address()))
                        .metadataDecoder(new LongMetadataDecoder())
                        .metadataEncoder(new LongMetadataEncoder())
                        .metadata(123L))
            .startAwait();
    System.out.println(
        "[" + joe.member().id() + "] Joe's metadata: " + joe.metadata().orElse(null));

    Cluster bob =
        new ClusterImpl()
            .config(
                config ->
                    config
                        .membership(opts -> opts.seedMembers(alice.address()))
                        .metadataDecoder(new LongMetadataDecoder())
                        .metadataEncoder(new LongMetadataEncoder())
                        .metadata(456L))
            .startAwait();
    System.out.println(
        "[" + bob.member().id() + "] Bob's metadata: " + bob.metadata().orElse(null));

    TimeUnit.SECONDS.sleep(3);

    alice
        .otherMembers()
        .forEach(
            member -> {
              Long metadata = (Long) alice.metadata(member).orElse(null);
              System.out.println(
                  "Alice knows [" + member.id() + "] has `" + metadata + "` as a metadata");
            });

    joe.otherMembers()
        .forEach(
            member -> {
              Long metadata = (Long) alice.metadata(member).orElse(null);
              System.out.println(
                  "Joe knows [" + member.id() + "] has `" + metadata + "` as a metadata");
            });

    bob.otherMembers()
        .forEach(
            member -> {
              Long metadata = (Long) alice.metadata(member).orElse(null);
              System.out.println(
                  "Bob knows [" + member.id() + "] has `" + metadata + "` as a metadata");
            });

    TimeUnit.SECONDS.sleep(3);
  }

  static class LongMetadataDecoder implements MetadataDecoder {
    @Override
    public Object decode(ByteBuffer buffer) {
      return buffer.getLong();
    }
  }

  static class LongMetadataEncoder implements MetadataEncoder {
    @Override
    public ByteBuffer encode(Object metadata) {
      if (metadata == null) {
        return null;
      }

      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong((Long) metadata);
      buffer.flip();
      return buffer;
    }
  }
}
