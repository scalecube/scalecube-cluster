package io.scalecube.transport.netty.websocket;

import static io.scalecube.cluster.transport.api.Transport.parseHost;
import static io.scalecube.cluster.transport.api.Transport.parsePort;

import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.transport.netty.Sender;
import io.scalecube.transport.netty.TransportImpl.SenderContext;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.WebsocketClientSpec;

public final class WebsocketSender implements Sender {

  private final TransportConfig config;

  public WebsocketSender(TransportConfig config) {
    this.config = config;
  }

  @Override
  public Mono<Connection> connect(String address) {
    return Mono.deferContextual(context -> Mono.just(context.get(SenderContext.class)))
        .map(context -> newWebsocketSender(context, address))
        .flatMap(sender -> sender.uri("/").connect());
  }

  @Override
  public Mono<Void> send(Message message) {
    return Mono.deferContextual(
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

  private HttpClient.WebsocketSender newWebsocketSender(SenderContext context, String address) {
    HttpClient httpClient =
        HttpClient.newConnection()
            .runOn(context.loopResources())
            .host(parseHost(address))
            .port(parsePort(address))
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeout());

    if (config.isClientSecured()) {
      httpClient = httpClient.secure();
    }

    return httpClient.websocket(
        WebsocketClientSpec.builder().maxFramePayloadLength(config.maxFrameLength()).build());
  }
}
