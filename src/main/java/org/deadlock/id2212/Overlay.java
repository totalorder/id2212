package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.protocol.IdJsonClient;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.asyncio.protocol.JsonProtocol;
import org.deadlock.id2212.messages.KnownPeers;
import org.deadlock.id2212.messages.PeerId;
import org.deadlock.id2212.messages.PeerInfo;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class Overlay implements Closeable {
  private final JsonProtocol jsonProtocol;
  private final List<Peer> peers = new LinkedList<>();
  private final UUID uuid;

  public Overlay(final JsonProtocol jsonProtocol) {
    this.jsonProtocol = jsonProtocol;
    jsonProtocol.registerType(PeerId.class);
    jsonProtocol.registerType(KnownPeers.class);
    uuid = UUID.randomUUID();
  }

  public CompletionStage<Peer> connect(InetSocketAddress inetSocketAddress) {
    return jsonProtocol.connect(inetSocketAddress)
      .thenCompose(this::exchangePeerId);
  }

  public CompletionStage<Peer> accept() {
    return jsonProtocol.accept()
        .thenCompose(this::exchangePeerId);
  }

  private CompletionStage<Peer> exchangePeerId(final IdJsonClient jsonClient) {
    // Send local peer id to remote peer
    return jsonClient.send(getPeerId())
        .thenCompose(ignored ->
            // Receive remote peer id
            jsonClient.receive())
        .thenApply(jsonMessage ->
            // Create peer with received peer id
            createPeer(jsonMessage.getObject(PeerId.class), jsonClient));
  }

  public CompletionStage<Void> announceKnownPeers() {
    List<CompletableFuture<Integer>> peerConnectedFutures;
    synchronized (peers) {
      peerConnectedFutures = peers.stream()
          .map(this::announceKnownPeers).map(CompletionStage::toCompletableFuture).collect(Collectors.toList());
    }

    return CompletableFuture.allOf(
        peerConnectedFutures.toArray(new CompletableFuture[peerConnectedFutures.size()]));
//    return allPeersConnected.thenCompose(ignored -> {
//      final int sumOfPeersConnected = peerConnectedFutures.stream()
//          .map(CompletableFuture::join)
//          .mapToInt(Integer::intValue)
//          .sum();
//      System.out.println(getUUID() + " connected to " + sumOfPeersConnected + " peers");
//      if (sumOfPeersConnected > 0) {
//        return announceKnownPeers().thenApply(discoveredPeers -> discoveredPeers + sumOfPeersConnected);
//      }
//      return completedFuture(sumOfPeersConnected);
//    });
  }

  private CompletionStage<Integer> announceKnownPeers(final Peer peer) {
    System.out.println(getUUID() + " sending " + getKnownPeers().peerInfos + " to " + peer.getPeerInfo().uuid);
    return peer
        // Sen local known peers
        .send(getKnownPeers())
        // Receive remote known peers
        .thenCompose(ignored -> peer.receive())
        .thenCompose(response -> {
          final Set<PeerInfo> newKnownPeers = response.getObject(KnownPeers.class).peerInfos
              // Filter out self
              .stream().filter(peerInfo -> !peerInfo.uuid.equals(uuid)).collect(Collectors.toSet());
//          System.out.println(getUUID() + " received " + newKnownPeers + " from " + peer.getPeerInfo().uuid);
          final Set<PeerInfo> existingKnownPeers = getKnownPeers().peerInfos;

          // Find new peers not known before
          newKnownPeers.removeAll(existingKnownPeers);

          System.out.println(getUUID() + " received new " + newKnownPeers + " from " + peer.getPeerInfo().uuid);

          // Connect to all new peers
          final List<CompletionStage<Peer>> newPeerFutures = newKnownPeers.stream()
              .filter(peerInfo -> peerInfo.uuid.compareTo(uuid) > 0)
              .map(peerInfo -> connect(peerInfo.address)).collect(Collectors.toList());
          if (newPeerFutures.size() == 0) {
            return completedFuture(0);
          }
          System.out.println(getUUID() + " connecting to " + newKnownPeers);
          final CompletionStage<Void> allNewPeersConnected = CompletableFuture.allOf(
              newPeerFutures.toArray(new CompletableFuture[newKnownPeers.size()]));

          // Return when all new are connected
          return allNewPeersConnected.thenApply(ignored -> {
            System.out.println(getUUID() + " connected to " + newKnownPeers);
            return newPeerFutures.size();
          });
        });
  }

  public CompletionStage<Void> receiveMessages() {
    final List<CompletionStage<Void>> peersAcceptedFutures;
    synchronized (peers) {
      peersAcceptedFutures = peers.stream()
          .map(peer -> peer.receive().thenCompose(jsonMessage -> {
            if (jsonMessage.isClass(KnownPeers.class)) {
              return acceptPeerAnnouncement(peer, jsonMessage.getObject(KnownPeers.class));
            } else {
              throw new RuntimeException("Unsupported message type!");
            }
          }))
          .collect(Collectors.toList());
    }

    return CompletableFuture.allOf(
        peersAcceptedFutures.toArray(new CompletableFuture[peersAcceptedFutures.size()]));
  }

  private CompletionStage<Void> acceptPeerAnnouncement(final Peer peer, final KnownPeers knownPeers) {
          System.out.println(getUUID() + " sending " + getKnownPeers().peerInfos + " to " + peer.getPeerInfo().uuid);
          final Set<PeerInfo> newKnownPeers = knownPeers.peerInfos
              // Filter out self
              .stream().filter(peerInfo -> !peerInfo.uuid.equals(uuid)).collect(Collectors.toSet());
//          System.out.println(getUUID() + " received " + newKnownPeers + " from " + peer.getPeerInfo().uuid);
          final Set<PeerInfo> existingKnownPeers = getKnownPeers().peerInfos;

          // Find new peers not known before
          newKnownPeers.removeAll(existingKnownPeers);

          System.out.println(getUUID() + " received new " + newKnownPeers + " from " + peer.getPeerInfo().uuid);

          // Connect to all new peers
          final List<CompletionStage<Peer>> newPeerFutures = newKnownPeers.stream()
              .filter(peerInfo -> peerInfo.uuid.compareTo(uuid) > 0)
              .map(peerInfo -> connect(peerInfo.address)).collect(Collectors.toList());
          if (newPeerFutures.size() == 0) {
            return completedFuture(null);
          }
          System.out.println(getUUID() + " connecting to " + newKnownPeers);
          final CompletionStage<Void> allNewPeersConnected = CompletableFuture.allOf(
              newPeerFutures.toArray(new CompletableFuture[newKnownPeers.size()]));

          // Return when all new are connected
          return allNewPeersConnected.thenApply(ignored -> {
            System.out.println(getUUID() + " connected to " + newKnownPeers);
            return null;
          });
  }

  public CompletionStage<Void> listen(final int port) {
    return jsonProtocol.startServer(port);
  }

  private Peer createPeer(final PeerId peerId, final IdJsonClient jsonClient) {
    final Peer peer = new Peer(peerId.uuid, peerId.listeningPort, jsonClient);
    synchronized (peers) {
      peers.add(peer);
    }
    return peer;
  }

  public KnownPeers getKnownPeers() {
    synchronized (peers) {
      return new KnownPeers( peers.stream().map(Peer::getPeerInfo).collect(Collectors.toSet()));
    }
  }

  public UUID getUUID() {
    return uuid;
  }

  private PeerId getPeerId() {
    return new PeerId(uuid, getListeningPort());
  }

  public int getListeningPort() {
    return jsonProtocol.getListeningPort();
  }

  @Override
  public void close() throws IOException {
    jsonProtocol.close();
  }

  public static Overlay createDefault() {
    return new Overlay(JsonProtocol.createDefault());
  }
}
