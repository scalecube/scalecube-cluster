package io.scalecube.cluster2.payload;

import static io.scalecube.cluster2.UUIDCodec.encodeUUID;

import io.scalecube.cluster2.sbe.PayloadGenerationHeaderEncoder;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2LongHashMap;

public class PayloadStore {

  private final PayloadGenerationHeaderEncoder headerEncoder = new PayloadGenerationHeaderEncoder();
  private final MutableDirectBuffer headerBuffer =
      new ExpandableDirectByteBuffer(headerEncoder.sbeBlockLength());
  private final ByteBuffer nullBuffer = ByteBuffer.wrap(new byte[1]);
  private final Object2LongHashMap<UUID> payloadIndex = new Object2LongHashMap<>(Long.MIN_VALUE);
  private FileChannel storeChannel;
  private RandomAccessFile storeFile;

  public PayloadStore(File storeFile) {
    try {
      this.storeFile = new RandomAccessFile(storeFile, "rw");
      this.storeChannel = this.storeFile.getChannel();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void addGeneration(UUID memberId, int payloadLength) throws IOException {
    payloadIndex.put(memberId, storeChannel.position());

    headerEncoder.wrap(headerBuffer, 0);
    encodeUUID(memberId, headerEncoder.memberId());
    headerEncoder.payloadLength(payloadLength);
    headerEncoder.payloadOffset(0);

    writeHeader(-1);

    for (int i = 0; i < payloadLength; i++) {
      //noinspection ResultOfMethodCallIgnored,RedundantCast
      storeChannel.write((ByteBuffer) nullBuffer.clear());
    }
  }

  public void removeGeneration(UUID memberId) throws IOException {
    final long position = payloadIndex.getValue(memberId);
    if (position != Long.MIN_VALUE) {
      readHeader(position);

      headerEncoder.wrap(headerBuffer, 0);
      headerEncoder.payloadOffset(Integer.MIN_VALUE);

      writeHeader(position);

      payloadIndex.removeKey(memberId);
    }
  }

  private void writeHeader(long position) throws IOException {
    //noinspection RedundantCast
    final ByteBuffer buffer = (ByteBuffer) headerBuffer.byteBuffer().clear();
    do {
      if (position >= 0) {
        storeChannel.write(buffer, position);
      } else {
        storeChannel.write(buffer);
      }
    } while (buffer.hasRemaining());
  }

  private void readHeader(long position) throws IOException {
    //noinspection RedundantCast
    final ByteBuffer buffer = (ByteBuffer) headerBuffer.byteBuffer().clear();
    do {
      storeChannel.read(buffer, position);
    } while (buffer.hasRemaining());
  }
}
