package org.deadlock.id2212.asyncio;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

public interface AsyncIOClient {
  CompletionStage<Void> send(final ByteBuffer byteBuffer);

  InetSocketAddress getAddress();

  void readyToReadAndWrite();
}
