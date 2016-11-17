package org.deadlock.id2212.asyncio.protocol;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public interface Client<Message> {
  void setOnMessageReceivedCallback(Consumer<Message> callback);

  InetSocketAddress getAddress();
}
