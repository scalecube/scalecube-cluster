//package io.scalecube.cluster2.payload;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.nio.ByteBuffer;
//import java.nio.channels.FileChannel;
//import java.util.UUID;
//import org.agrona.CloseHelper;
//import org.agrona.DirectBuffer;
//import org.agrona.collections.Object2ObjectHashMap;
//import org.agrona.concurrent.broadcast.BroadcastTransmitter;
//
//public class PayloadStore implements AutoCloseable {
//
//  private final BroadcastTransmitter messageTx;
//
//  private final RandomAccessFile storeFile;
//  private final FileChannel storeChannel;
//  private final PayloadCodec payloadCodec = new PayloadCodec();
//  // private final ByteBuffer dstBuffer = ByteBuffer.allocateDirect(64 * 1024);
//  private final Object2ObjectHashMap<UUID, PayloadInfo> payloadIndex = new Object2ObjectHashMap<>();
//
//  public PayloadStore(File payloadStore, BroadcastTransmitter messageTx) {
//    try {
//      this.messageTx = messageTx;
//      storeFile = new RandomAccessFile(payloadStore, "rw");
//      storeChannel = storeFile.getChannel();
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  public void addGeneration(UUID memberId, int payloadLength) throws IOException {
//    if (payloadLength == 0) {
//      payloadIndex.put(memberId, PayloadInfo.NULL_INSTANCE);
//      emitPayloadCreatedEvent(memberId);
//      return;
//    }
//
//    payloadIndex.put(memberId, new PayloadInfo(memberId, storeChannel.position(), payloadLength));
//
//    // Fill up with 0
//
//    for (int i = 0; i < payloadLength; i++) {
//      storeFile.write(0);
//    }
//  }
//
//  public void removeGeneration(UUID memberId) {
//    payloadIndex.remove(memberId);
//  }
//
//  public int size() {
//    return payloadIndex.size();
//  }
//
//  private void emitPayloadCreatedEvent(UUID memberId) {
//    // TODO
//  }
//
//  @Override
//  public void close() {
//    CloseHelper.quietCloseAll(storeFile, storeChannel);
//  }
//}
