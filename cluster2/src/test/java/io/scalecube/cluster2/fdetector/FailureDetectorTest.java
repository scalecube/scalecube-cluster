package io.scalecube.cluster2.fdetector;

import static org.agrona.concurrent.broadcast.BroadcastBufferDescriptor.TRAILER_LENGTH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster.transport.api2.Transport.MessagePoller;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.membership.MembershipEventCodec;
import io.scalecube.cluster2.sbe.MembershipEventType;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.function.Supplier;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FailureDetectorTest {

  private final Member localMember = new Member(UUID.randomUUID(), "local", "address:1180", "ns");
  private final Member fooMember = new Member(UUID.randomUUID(), "foo", "address:1181", "ns");
  private final Member barMember = new Member(UUID.randomUUID(), "bar", "address:1182", "ns");
  private final Member aliceMember = new Member(UUID.randomUUID(), "alice", "address:1183", "ns");
  private final Member bobMember = new Member(UUID.randomUUID(), "bob", "address:1184", "ns");

  private final Transport transport = mock(Transport.class);
  private final ExpandableDirectByteBuffer byteBuffer =
      new ExpandableDirectByteBuffer(1024 * 1024 + TRAILER_LENGTH);
  private final BroadcastTransmitter messageTx =
      new BroadcastTransmitter(new UnsafeBuffer(byteBuffer));
  private final Supplier<CopyBroadcastReceiver> messageRxSupplier =
      () -> new CopyBroadcastReceiver(new BroadcastReceiver(new UnsafeBuffer(byteBuffer)));
  private final CachedEpochClock epochClock = new CachedEpochClock();
  private final FailureDetectorConfig config = new FailureDetectorConfig();
  private final FailureDetector failureDetector =
      new FailureDetector(transport, messageTx, messageRxSupplier, epochClock, config, localMember);
  private final MembershipEventCodec membershipEventCodec = new MembershipEventCodec();
  private final MessagePoller messagePoller = mock(MessagePoller.class);

  @BeforeEach
  void beforeEach() {
    when(transport.newMessagePoller()).thenReturn(messagePoller);
  }

  private void doOnMessagePoller(Runnable runnable) {
    doAnswer(
            invocation -> {
              runnable.run();
              final Method method = invocation.getMethod();
              final Object mock = invocation.getMock();
              final Object[] arguments = invocation.getArguments();
              return method.invoke(mock, arguments);
            })
        .when(messagePoller)
        .poll(any());
  }

  private void emitMembershipEvent(MembershipEventType eventType, Member member) {
    messageTx.transmit(
        1,
        membershipEventCodec.encodeMembershipEvent(eventType, 1, member),
        0,
        membershipEventCodec.encodedLength());
  }

  @Test
  void testTickWhenNoPingMembers() {
    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMembershipEventLocalMemberWillBeFiltered() {
    emitMembershipEvent(MembershipEventType.ADDED, localMember);
    failureDetector.doWork();

    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMembershipEventAddedThenRemoved() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    failureDetector.doWork();
    failureDetector.doWork();
    failureDetector.doWork();

    emitMembershipEvent(MembershipEventType.REMOVED, fooMember);
    failureDetector.doWork();

    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMembershipEventAddedThenLeaving() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    failureDetector.doWork();
    failureDetector.doWork();
    failureDetector.doWork();

    emitMembershipEvent(MembershipEventType.LEAVING, fooMember);
    failureDetector.doWork();

    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }
}
