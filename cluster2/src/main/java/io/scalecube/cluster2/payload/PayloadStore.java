package io.scalecube.cluster2.payload;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

public class PayloadStore implements AutoCloseable {

  private final RandomAccessFile storeFile;
  private final FileChannel storeChannel;
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

  public boolean putPayload(
      UUID memberId, int payloadOffset, DirectBuffer chunk, int chunkOffset, int chunkLength)
      throws IOException {
    final PayloadInfo payloadInfo = payloadIndex.get(memberId);
    if (payloadInfo == null) {
      throw new IllegalArgumentException("Payload not found, memberId: " + memberId);
    }

    if (payloadOffset != payloadInfo.appendOffset()) {
      throw new IllegalArgumentException("Invalid payloadOffset: " + payloadOffset);
    }

    final int newAppendOffset = payloadInfo.appendOffset() + chunkLength;

    if (newAppendOffset > payloadInfo.length()) {
      throw new IllegalArgumentException("Invalid chunkLength: " + chunkLength);
    }

    //noinspection RedundantCast
    chunk.getBytes(chunkOffset, (ByteBuffer) dstBuffer.clear(), chunkLength);

    dstBuffer.flip();
    long position = payloadInfo.appendPosition();
    do {
      position += storeChannel.write(dstBuffer, position);
    } while (dstBuffer.hasRemaining());

    payloadInfo.appendOffset(newAppendOffset);

    return payloadInfo.isCompleted();
  }

  public ByteBuffer readPayload(UUID memberId) throws IOException {
    final PayloadInfo payloadInfo = payloadIndex.get(memberId);
    if (payloadInfo == null || !payloadInfo.isCompleted()) {
      return null;
    }

    //noinspection RedundantCast
    final ByteBuffer readBuffer = (ByteBuffer) dstBuffer.clear().limit(payloadInfo.length());
    long position = payloadInfo.position();
    do {
      position += storeChannel.read(readBuffer, position);
    } while (readBuffer.hasRemaining());

    //noinspection RedundantCast
    return (ByteBuffer) readBuffer.flip();
  }

  public int size() {
    return payloadIndex.size();
  }

  @Override
  public void close() {
    CloseHelper.quietCloseAll(storeFile, storeChannel);
  }
}
