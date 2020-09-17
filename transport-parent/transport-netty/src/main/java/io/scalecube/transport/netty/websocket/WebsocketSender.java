package io.scalecube.transport.netty.websocket;

import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.Sender;
import io.scalecube.transport.netty.TransportImpl.SenderContext;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.WebsocketClientSpec;
import reactor.netty.tcp.TcpClient;

final class WebsocketSender implements Sender {

  private final TransportConfig config;

  WebsocketSender(TransportConfig config) {
    this.config = config;
  }

  @Override
  public Mono<Connection> connect(Address address) {
    return Mono.deferWithContext(context -> Mono.just(context.get(SenderContext.class)))
        .map(context -> newWebsocketSender(context, address))
        .flatMap(sender -> sender.uri("/").connect());
  }

  @Override
  public Mono<Void> send(Message message) {
    return Mono.deferWithContext(
        context -> {
          Connection connection = context.get(Connection.class);
          SenderContext senderContext = context.get(SenderContext.class);
          return connection
              .outbound()
              .sendObject(
                  Mono.just(message)
                      .map(senderContext.messageEncoder())
                      .map(BinaryWebSocketFrame::new),
                  bb -> true)
              .then();
        });
  }

  private HttpClient.WebsocketSender newWebsocketSender(SenderContext context, Address address) {
    return HttpClient.newConnection()
        .tcpConfiguration(
            tcpClient -> {
              TcpClient tcpClient1 =
                  tcpClient
                      .runOn(context.loopResources())
                      .host(address.host())
                      .port(address.port())
                      .option(ChannelOption.TCP_NODELAY, true)
                      .option(ChannelOption.SO_KEEPALIVE, true)
                      .option(ChannelOption.SO_REUSEADDR, true)
                      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeout());
              return config.isSecured() ? tcpClient1.secure() : tcpClient1;
            })
        .websocket(
            WebsocketClientSpec.builder().maxFramePayloadLength(config.maxFrameLength()).build());
  }
}
