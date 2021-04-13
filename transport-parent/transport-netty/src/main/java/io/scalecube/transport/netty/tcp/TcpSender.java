package io.scalecube.transport.netty.tcp;

import io.netty.channel.ChannelOption;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.Sender;
import io.scalecube.transport.netty.TransportImpl.SenderContext;
import java.time.Duration;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

final class TcpSender implements Sender {

  private final TransportConfig config;

  TcpSender(TransportConfig config) {
    this.config = config;
  }

  @Override
  public Mono<Connection> connect(Address address) {
    return Mono.deferWithContext(context -> Mono.just(context.get(SenderContext.class)))
        .map(context -> newTcpClient(context, address))
        .flatMap(TcpClient::connect);
  }

  @Override
  public Mono<Void> send(Message message) {
    return Mono.deferWithContext(
        context -> {
          Connection connection = context.get(Connection.class);
          SenderContext senderContext = context.get(SenderContext.class);
          return connection
              .outbound()
              .sendObject(Mono.just(message).map(senderContext.messageEncoder()), bb -> true)
              .then();
        });
  }

  private TcpClient newTcpClient(SenderContext context, Address address) {
    TcpClient tcpClient =
        TcpClient.newConnection()
            .runOn(context.loopResources())
            .host(address.host())
            .port(address.port())
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeout())
            .resolver(opts -> opts.cacheMaxTimeToLive(Duration.ZERO))
            .doOnChannelInit(
                (connectionObserver, channel, remoteAddress) ->
                    new TcpChannelInitializer(config.maxFrameLength())
                        .accept(connectionObserver, channel));
    return config.isClientSecured() ? tcpClient.secure() : tcpClient;
  }
}
