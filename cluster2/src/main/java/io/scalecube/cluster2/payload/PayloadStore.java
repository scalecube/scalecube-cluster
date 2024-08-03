package io.scalecube.cluster2.payload;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.StringJoiner;
import java.util.UUID;
import org.agrona.DirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

public class PayloadStore {

  private FileChannel storeChannel;
  private RandomAccessFile storeFile;
  private final ByteBuffer dstBuffer = ByteBuffer.allocateDirect(64 * 1024);
  private final Object2ObjectHashMap<UUID, PayloadInfo> payloadIndex = new Object2ObjectHashMap<>();

  public PayloadStore(File payloadStore) {
    try {
      storeFile = new RandomAccessFile(payloadStore, "rw");
      storeChannel = storeFile.getChannel();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void addGeneration(UUID memberId, int payloadLength) throws IOException {
    payloadIndex.put(
        memberId, new PayloadInfo(memberId, storeChannel.position(), payloadLength, 0));

    // Fill up with 0

    for (int i = 0; i < payloadLength; i++) {
      storeFile.write(0);
    }
  }

  public void removeGeneration(UUID memberId) {
    payloadIndex.remove(memberId);
  }

  public int size() {
    return payloadIndex.size();
  }

  public void putPayload(UUID memberId, DirectBuffer chunk, int chunkOffset, int chunkLength)
      throws IOException {
    final PayloadInfo payloadInfo = payloadIndex.get(memberId);
    if (payloadInfo == null || payloadInfo.isCompleted()) {
      return;
    }

    if (payloadInfo.length < payloadInfo.appendOffset + chunkLength) {
      throw new IllegalArgumentException("Invalid chunkLength: " + chunkLength);
    }

    //noinspection RedundantCast
    chunk.getBytes(chunkOffset, (ByteBuffer) dstBuffer.clear(), chunkLength);

    dstBuffer.flip();
    long position = payloadInfo.appendPosition();
    do {
      position += storeChannel.write(dstBuffer, position);
    } while (dstBuffer.hasRemaining());

    payloadInfo.appendOffset += chunkLength;
  }

  private static class PayloadInfo {

    private final UUID memberId;
    private final long initialPosition;
    private final int length;
    private int appendOffset;

    private PayloadInfo(UUID memberId, long initialPosition, int length, int appendOffset) {
      this.memberId = memberId;
      this.initialPosition = initialPosition;
      this.length = length;
      this.appendOffset = appendOffset;
    }

    private long appendPosition() {
      return initialPosition + appendOffset;
    }

    private boolean isCompleted() {
      return appendOffset == length;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", PayloadInfo.class.getSimpleName() + "[", "]")
          .add("memberId=" + memberId)
          .add("initialPosition=" + initialPosition)
          .add("length=" + length)
          .add("proceedingOffset=" + appendOffset)
          .toString();
    }
  }
}
