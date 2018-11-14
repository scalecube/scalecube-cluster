package io.scalecube.examples;

import java.net.InetSocketAddress;
import java.time.Duration;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.NettyPipeline.SendOptions;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

public class RawTcpServerClientRunner {

  public static void main(String[] args) throws InterruptedException {
    InetSocketAddress address = new InetSocketAddress(7071);

    LoopResources serverResources = LoopResources.create("server", 1, true);
    LoopResources clientResources = LoopResources.create("client", 1, true);

    DisposableServer server =
        TcpServer.create()
            .runOn(serverResources)
            .addressSupplier(() -> address)
            .handle(
                (in, out) -> {
                  in.receive().asString().log("server receive").subscribe();
                  return Mono.never();
                })
            .bind()
            .block();

    ConnectionProvider connectionProvider = ConnectionProvider.fixed("client", 1);

    long l = System.currentTimeMillis();

    Connection connection = getOrConnect(address, connectionProvider, clientResources);

    send(connection, "hello 1");
    //Thread.sleep(100);
    send(connection, "hello 2");
    //Thread.sleep(100);
    send(connection, "hello 3");
    //Thread.sleep(100);

    Mono.delay(Duration.ofSeconds(6))
        .subscribe(
            aLong -> {
              server.dispose();
              server.onDispose(() -> System.err.println("!!! server.dispose()"));
              serverResources.dispose();
            });

    Thread.currentThread().join();
  }

  private static void send(Connection conn, String data) {
    conn.outbound()
        .options(SendOptions::flushOnEach)
        .sendString(Mono.just(data))
        .then()
        .subscribe();
  }

  private static Connection getOrConnect(
      InetSocketAddress address,
      ConnectionProvider connectionProvider,
      LoopResources clientRespurces) {
    return TcpClient.create(connectionProvider)
        .runOn(clientRespurces)
        .host(address.getHostString())
        .port(address.getPort())
        .observe((conn, newState) -> System.out.println("### " + conn + " | " + newState))
        .connectNow();
  }
}
