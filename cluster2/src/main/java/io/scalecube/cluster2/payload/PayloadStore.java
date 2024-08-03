package io.scalecube.cluster2.payload;

import static io.scalecube.cluster2.UUIDCodec.encodeUUID;

import io.scalecube.cluster2.sbe.PayloadGenerationHeaderDecoder;
import io.scalecube.cluster2.sbe.PayloadGenerationHeaderEncoder;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2LongHashMap;
import org.agrona.collections.ObjectHashSet;

public class PayloadStore {

  public static final int HEADER_LENGTH = PayloadGenerationHeaderEncoder.BLOCK_LENGTH;

  private final PayloadGenerationHeaderEncoder headerEncoder = new PayloadGenerationHeaderEncoder();
  private final PayloadGenerationHeaderDecoder headerDecoder = new PayloadGenerationHeaderDecoder();
  private final MutableDirectBuffer headerBuffer = new ExpandableDirectByteBuffer(HEADER_LENGTH);
  private final ByteBuffer nullBuffer = ByteBuffer.wrap(new byte[1]);
  private final Object2LongHashMap<UUID> payloadIndex = new Object2LongHashMap<>(Long.MIN_VALUE);
  private final ObjectHashSet<UUID> proceedingMembers = new ObjectHashSet<>();
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
    proceedingMembers.add(memberId);

    headerEncoder.wrap(headerBuffer, 0);
    encodeUUID(memberId, headerEncoder.memberId());
    headerEncoder.payloadLength(payloadLength);
    headerEncoder.payloadOffset(0);

    appendHeader();

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
      proceedingMembers.remove(memberId);
    }
  }

  public void putPayload(
      UUID memberId, int payloadOffset, DirectBuffer chunk, int chunkOffset, int chunkLength)
      throws IOException {
    final long position = payloadIndex.getValue(memberId);
    if (position != Long.MIN_VALUE) {
      readHeader(position);

      headerEncoder.wrap(headerBuffer, 0);
      headerDecoder.wrap(headerBuffer, 0, HEADER_LENGTH, 0);

      final long payloadPosition = position + HEADER_LENGTH + payloadOffset;
    }
  }

  private void appendHeader() throws IOException {
    //noinspection RedundantCast
    final ByteBuffer buffer = (ByteBuffer) headerBuffer.byteBuffer().clear();
    do {
      storeChannel.write(buffer);
    } while (buffer.hasRemaining());
  }

  private void writeHeader(long position) throws IOException {
    //noinspection RedundantCast
    final ByteBuffer buffer = (ByteBuffer) headerBuffer.byteBuffer().clear();
    do {
      storeChannel.write(buffer, position);
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
