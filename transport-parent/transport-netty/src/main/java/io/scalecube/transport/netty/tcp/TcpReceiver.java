package io.scalecube.transport.netty.tcp;

import io.netty.channel.ChannelOption;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.transport.netty.Receiver;
import io.scalecube.transport.netty.TransportImpl.ReceiverContext;
import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;

final class TcpReceiver implements Receiver {

  private final TransportConfig config;

  TcpReceiver(TransportConfig config) {
    this.config = config;
  }

  @Override
  public Mono<DisposableServer> bind() {
    return Mono.deferWithContext(context -> Mono.just(context.get(ReceiverContext.class)))
        .flatMap(
            context ->
                newTcpServer(context)
                    .handle(
                        (in, out) ->
                            in.receive()
                                .retain()
                                .map(context.messageDecoder())
                                .doOnNext(context::onMessage)
                                .then())
                    .bind()
                    .cast(DisposableServer.class));
  }

  private TcpServer newTcpServer(ReceiverContext context) {
    return TcpServer.create()
        .runOn(context.loopResources())
        .bindAddress(() -> new InetSocketAddress(config.port()))
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.SO_REUSEADDR, true)
        .doOnChannelInit(
            (connectionObserver, channel, remoteAddress) -> {
              new TcpChannelInitializer(config.maxFrameLength())
                  .accept(connectionObserver, channel);
            });
  }
}
