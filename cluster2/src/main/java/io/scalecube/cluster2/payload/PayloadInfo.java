package io.scalecube.cluster2.payload;

import java.util.StringJoiner;
import java.util.UUID;

public class PayloadInfo {

  private final UUID memberId;
  private final long position;
  private final int length;
  private int appendOffset;

  public PayloadInfo(UUID memberId, long position, int length, int appendOffset) {
    this.memberId = memberId;
    this.position = position;
    this.length = length;
    this.appendOffset = appendOffset;
  }

  public UUID memberId() {
    return memberId;
  }

  public long position() {
    return position;
  }

  public int length() {
    return length;
  }

  public int appendOffset() {
    return appendOffset;
  }

  public PayloadInfo appendOffset(int appendOffset) {
    this.appendOffset = appendOffset;
    return this;
  }

  public long appendPosition() {
    return position + appendOffset;
  }

  public boolean isCompleted() {
    return appendOffset == length;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", PayloadInfo.class.getSimpleName() + "[", "]")
        .add("memberId=" + memberId)
        .add("initialPosition=" + position)
        .add("length=" + length)
        .add("proceedingOffset=" + appendOffset)
        .toString();
  }
}
