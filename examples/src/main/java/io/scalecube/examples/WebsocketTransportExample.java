package io.scalecube.examples;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.net.Address;
import io.scalecube.transport.websocket.TransportImpl;

public class WebsocketTransportExample {

  public static void main(String[] args) throws InterruptedException {
    Transport t1 = TransportImpl.bindAwait();
    Transport t2 = TransportImpl.bindAwait();

    t2.listen().subscribe(System.err::println);

    t1.send(Address.from("localhost:9090"), Message.builder().data("hola").build()).block();

    Thread.sleep(1000);

    t2.stop().block();
    System.out.println(t2.isStopped());

    Thread.currentThread().join();
  }
}
