package io.scalecube.cluster;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class CorellationIdGenerator {
  private final String cidPrefix;
  private AtomicLong counter = new AtomicLong();

  public CorellationIdGenerator(String cidPrefix) {
    this.cidPrefix = Objects.requireNonNull(cidPrefix, "cidPrefix");
  }

  public String nextCid() {
    return cidPrefix + "-" + counter.incrementAndGet();
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("CorellationIdGenerator{");
    sb.append("cidPrefix='").append(cidPrefix).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
