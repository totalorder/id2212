package org.deadlock.id2212.asyncio.protocol;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

public interface Protocol<Client> extends Closeable {
  CompletionStage<Client> accept();

  CompletionStage<Void> startServer(int port);

  int getListeningPort();

  CompletionStage<Client> connect(InetSocketAddress inetSocketAddress);
}
