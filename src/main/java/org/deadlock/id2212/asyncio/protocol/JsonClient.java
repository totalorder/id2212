package org.deadlock.id2212.asyncio.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

public interface JsonClient<MessageType> {
  CompletionStage<Void> send(final Object serializable);

  CompletionStage<MessageType> receive();

  InetSocketAddress getAddress();
}
