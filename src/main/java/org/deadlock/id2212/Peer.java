package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.protocol.IdJsonClient;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.messages.PeerInfo;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class Peer {
  private final UUID uuid;
  private final int listeningPort;
  private final IdJsonClient jsonClient;

  public Peer(final UUID uuid, int listeningPort, final IdJsonClient jsonClient) {
    this.uuid = uuid;
    this.listeningPort = listeningPort;
    this.jsonClient = jsonClient;
  }

  public CompletionStage<Void> send(Object object) {
    return jsonClient.send(object);
  }

  public CompletionStage<IdJsonMessage> receive() {
    return jsonClient.receive();
  }

  public PeerInfo getPeerInfo() {
    final InetSocketAddress listeningAddress = new InetSocketAddress(
        jsonClient.getAddress().getAddress(), listeningPort);
    return new PeerInfo(uuid, listeningAddress);
  }
}
