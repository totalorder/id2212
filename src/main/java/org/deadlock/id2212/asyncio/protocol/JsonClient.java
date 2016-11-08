package org.deadlock.id2212.asyncio.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.concurrent.CompletionStage;

public interface JsonClient {
  CompletionStage<Void> send(final Object serializable) throws JsonProcessingException;

  <T> CompletionStage<T> receive(final Class<T> clazz);
}
