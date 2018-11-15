package io.scalecube.examples;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportConfig;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Created by Artem Vysochyn */
public class Example2 {

  public static void main(String[] args)
    throws InterruptedException, ExecutionException, TimeoutException {
    Transport server = Transport.bindAwait(TransportConfig.builder().connectTimeout(3000).build());
    Transport client = Transport.bindAwait(TransportConfig.builder().connectTimeout(3000).build());

    server
        .listen()
        .buffer(2)
        .subscribe(
            messages -> {
              System.err.println(">>> receive messages: " + messages);
              for (Message message : messages) {
                Address address = message.sender();
                server.send(address, Message.fromData("echo/" + message.qualifier())).subscribe();
              }
            });

    final CompletableFuture<List<Message>> targetFuture = new CompletableFuture<>();
    client.listen().buffer(2).subscribe(targetFuture::complete);

    client.send(server.address(), Message.fromData("q1")).subscribe();
    client.send(server.address(), Message.fromData("q2")).subscribe();

    List<Message> target = targetFuture.get(3, TimeUnit.SECONDS);
    System.err.println("### target list: " + target);
  }
}
