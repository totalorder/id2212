package org.deadlock.id2212.asyncio.protocol;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public interface Client<Message> extends Closeable {
  void setOnMessageReceivedCallback(Consumer<Message> callback);
  void setOnBrokenPipeCallback(Runnable callback);

  InetSocketAddress getAddress();

  void onBrokenPipe();
}
