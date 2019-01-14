package io.scalecube.cluster;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class CorrelationIdGenerator {
  private final String cidPrefix;
  private final AtomicLong counter = new AtomicLong(System.currentTimeMillis());

  public CorrelationIdGenerator(String cidPrefix) {
    this.cidPrefix = Objects.requireNonNull(cidPrefix, "cidPrefix");
  }

  public String nextCid() {
    return cidPrefix + "-" + counter.incrementAndGet();
  }
}
