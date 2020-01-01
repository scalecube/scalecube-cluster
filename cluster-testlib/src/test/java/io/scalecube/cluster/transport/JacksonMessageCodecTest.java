package io.scalecube.cluster.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class JacksonMessageCodecTest {

  private static final MessageCodec messageCodec = MessageCodec.INSTANCE;
  private static final Random random = new Random();

  @Test
  void serializeAndDeserializeByteBuffer() throws Exception {
    byte[] bytes = "hello".getBytes();

    Message to = Message.builder().data(new Entity(ByteBuffer.wrap(bytes))).build();
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    messageCodec.serialize(to, output);

    Message from = messageCodec.deserialize(new ByteArrayInputStream(output.toByteArray()));
    Entity entity = from.data();

    assertArrayEquals(bytes, entity.getMetadata().array());
  }

  @Test
  void serializeAndDeserializeDirectByteBuffer() throws Exception {
    byte[] bytes = new byte[512];
    random.nextBytes(bytes);

    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length * 4);
    byteBuffer.put(bytes);
    byteBuffer.flip();

    Message to = Message.builder().data(new Entity(byteBuffer)).build();
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    messageCodec.serialize(to, output);

    Message from = messageCodec.deserialize(new ByteArrayInputStream(output.toByteArray()));
    Entity entity = from.data();

    assertArrayEquals(bytes, entity.getMetadata().array());
  }

  @Test
  void serializeAndDeserializeEmptyByteBuffer() throws Exception {
    byte[] bytes = new byte[0];

    Message to = Message.builder().data(new Entity(ByteBuffer.wrap(bytes))).build();
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    messageCodec.serialize(to, output);

    Message from = messageCodec.deserialize(new ByteArrayInputStream(output.toByteArray()));
    Entity entity = from.data();

    assertArrayEquals(bytes, entity.getMetadata().array());
  }

  @Test
  @Disabled("https://github.com/FasterXML/jackson-databind/issues/1662")
  void serializeAndDeserializeByteBufferWithOffset() throws Exception {
    byte[] bytes = new byte[512];
    random.nextBytes(bytes);
    int offset = random.nextInt(bytes.length / 2);

    byte[] expected = Arrays.copyOfRange(bytes, offset, bytes.length);

    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, offset, bytes.length - offset);

    assertEquals(offset, byteBuffer.position());
    assertEquals(bytes.length - offset, byteBuffer.remaining());

    Message to = Message.builder().data(new Entity(byteBuffer)).build();
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    messageCodec.serialize(to, output);

    Message from = messageCodec.deserialize(new ByteArrayInputStream(output.toByteArray()));
    Entity entity = from.data();

    assertArrayEquals(expected, entity.getMetadata().array());
  }

  @Test
  void serializeAndDeserializeByteBufferWithOffsetSlice() throws Exception {
    byte[] bytes = new byte[512];
    random.nextBytes(bytes);
    int offset = random.nextInt(bytes.length / 2);

    byte[] expected = Arrays.copyOfRange(bytes, offset, bytes.length);

    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, offset, bytes.length - offset).slice();

    assertEquals(0, byteBuffer.position());
    assertEquals(bytes.length - offset, byteBuffer.remaining());

    Message to = Message.builder().data(new Entity(byteBuffer)).build();
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    messageCodec.serialize(to, output);

    Message from = messageCodec.deserialize(new ByteArrayInputStream(output.toByteArray()));
    Entity entity = from.data();

    assertArrayEquals(expected, entity.getMetadata().array());
  }

  @Test
  @Disabled("https://github.com/FasterXML/jackson-databind/issues/1662")
  void serializeAndDeserializeDirectByteBufferWithOffset() throws Exception {
    byte[] bytes = new byte[512];
    random.nextBytes(bytes);
    int offset = random.nextInt(bytes.length / 2);

    byte[] expected = Arrays.copyOfRange(bytes, offset, bytes.length);

    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length * 4);
    byteBuffer.put(bytes);
    byteBuffer.flip();
    byteBuffer.position(offset);

    assertEquals(offset, byteBuffer.position());
    assertEquals(bytes.length - offset, byteBuffer.remaining());

    Message to = Message.builder().data(new Entity(byteBuffer)).build();
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    messageCodec.serialize(to, output);

    Message from = messageCodec.deserialize(new ByteArrayInputStream(output.toByteArray()));
    Entity entity = from.data();

    assertArrayEquals(expected, entity.getMetadata().array());
  }

  @Test
  void serializeAndDeserializeDirectByteBufferWithOffsetSlice() throws Exception {
    byte[] bytes = new byte[512];
    random.nextBytes(bytes);
    int offset = random.nextInt(bytes.length / 2);

    byte[] expected = Arrays.copyOfRange(bytes, offset, bytes.length);

    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length * 4);
    byteBuffer.put(bytes);
    byteBuffer.flip();
    byteBuffer.position(offset);
    ByteBuffer slice = byteBuffer.slice();

    assertEquals(0, slice.position());
    assertEquals(bytes.length - offset, slice.remaining());

    Message to = Message.builder().data(new Entity(slice)).build();
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    messageCodec.serialize(to, output);

    Message from = messageCodec.deserialize(new ByteArrayInputStream(output.toByteArray()));
    Entity entity = from.data();

    assertArrayEquals(expected, entity.getMetadata().array());
  }

  @Test
  @Disabled("Cannot construct instance of java.nio.HeapByteBuffer (no Creators, default construct)")
  void serializeAndDeserializeByteBufferWithoutEntity() throws Exception {
    byte[] bytes = "hello".getBytes();

    Message to = Message.builder().data(ByteBuffer.wrap(bytes)).build();
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    messageCodec.serialize(to, output);

    Message from = messageCodec.deserialize(new ByteArrayInputStream(output.toByteArray()));
    ByteBuffer byteBuffer = from.data();

    assertArrayEquals(bytes, byteBuffer.array());
  }

  static final class Entity {
    private ByteBuffer metadata;

    Entity() {}

    Entity(ByteBuffer metadata) {
      this.metadata = metadata;
    }

    ByteBuffer getMetadata() {
      return metadata;
    }
  }
}
