package io.scalecube.examples;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.transport.netty.TransportImpl;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;

public class WebsocketTransportExample {

  public static void main(String[] args) throws InterruptedException {
    Transport t1 =
        TransportImpl.bindAwait(
            TransportConfig.defaultConfig().transportFactory(new WebsocketTransportFactory()));

    Transport t2 =
        TransportImpl.bindAwait(
            TransportConfig.defaultConfig().transportFactory(new WebsocketTransportFactory()));
    t2.listen().subscribe(System.err::println);

    t1.send(t2.address(), Message.builder().data("hola1").build()).block();
    t1.send(t2.address(), Message.builder().data("hola2").build()).block();
    t1.send(t2.address(), Message.builder().data("hola3").build()).block();

    Thread.sleep(1000);

    t2.stop().block();
    System.out.println(t2.isStopped());

    t2 =
        TransportImpl.bindAwait(
            TransportConfig.defaultConfig().transportFactory(new WebsocketTransportFactory()));
    t2.listen().subscribe(System.err::println);

    t1.send(t2.address(), Message.builder().data("hola4").build()).block();
    t1.send(t2.address(), Message.builder().data("hola5").build()).block();
    t1.send(t2.address(), Message.builder().data("hola6").build()).block();

    Thread.sleep(1000);

    Thread.currentThread().join();
  }
}
