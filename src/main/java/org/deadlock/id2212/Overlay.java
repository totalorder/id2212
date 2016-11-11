package org.deadlock.id2212;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

public interface Overlay extends Closeable {
  CompletionStage<Peer> connect(InetSocketAddress inetSocketAddress);

  CompletionStage<Void> waitForConnectedPeers(int numberOfPeers);

  CompletionStage<Void> start(int port);

  int getListeningPort();
}
