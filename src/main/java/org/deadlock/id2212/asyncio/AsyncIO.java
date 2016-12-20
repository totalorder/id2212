package org.deadlock.id2212.asyncio;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface AsyncIO extends Closeable {

  void setClientDataReceivedCallback(BiConsumer<AsyncIOClient, ByteBuffer> clientDataReceivedCallback);
  void setClientBrokenPipeCallback(Consumer<AsyncIOClient> clientBrokenPipeCallback);

  CompletionStage<Void> startServer(int port,
                                    Consumer<AsyncIOClient> clientAcceptedCallback);

  CompletionStage<AsyncIOClient> connect(InetSocketAddress address);

  int getListeningPort();

  InetSocketAddress getListeningAddress();

  class BrokenPipeException extends Exception {}
}
