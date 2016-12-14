package org.deadlock.id2212;

import com.google.common.collect.Lists;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.messages.RequestSchedule;
import org.deadlock.id2212.overlay.Overlay;
import org.deadlock.id2212.overlay.Peer;
import org.deadlock.id2212.overlay.PeerExchangeOverlay;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Find a common time among N peers in schedule X
 *
 * Automatically discovers other peers in the overlay after connecting
 * to a bootstrap node.
 */
public class DHT implements Closeable {
  private final Overlay overlay;
  private final LinkedList<Peer> fingers = new LinkedList<>();
  private final UUID localUUID;

  public DHT(final Overlay overlay) {
    overlay.setOnMessageReceivedCallback(this::onMessageReceived);
    overlay.setOnPeerAcceptedCallback(this::onPeerAccepted);
    overlay.setOnPeerConnectedCallback(this::onPeerConnected);
//    overlay.registerType(Schedule.class);
//    overlay.registerType(RequestSchedule.class);
    this.overlay = overlay;
    localUUID = overlay.getUUID();
  }

  private void onMessageReceived(final Peer peer, final IdJsonMessage message) {
    // Respond with own schedule to RequestSchedule messages
    if (message.isClass(RequestSchedule.class)) {
//      peer.send(mySchedule);
//    } else if (message.isClass(Schedule.class)) {
//      final CompletableFuture<Schedule> scheduleFuture;
//      synchronized (waitingForSchedules) {
//        waitingForSchedules.putIfAbsent(peer, new CompletableFuture<>());
//        scheduleFuture = waitingForSchedules.get(peer);
//      }
//      scheduleFuture.complete(message.getObject(Schedule.class));
    }
  }

  private void onPeerAccepted(final Peer peer) {
    System.out.println("Accepted connection from " + peer.getUUID().toString());
  }

  private void onPeerConnected(final Peer peer) {
    System.out.println("Connected to " + peer.getUUID().toString());
  }

  public static DHT createDefault() {
    return new DHT(PeerExchangeOverlay.createDefault());
  }

  public CompletionStage<Void> start(final int port) {
    return overlay.start(port).thenApply(ignored -> {
      System.out.println("UUID: " + overlay.getUUID());
      System.out.println("Listening on port " + port + "...");
      return null;
    });
  }

  public int getListeningPort() {
    return overlay.getListeningPort();
  }

  public CompletionStage<Void> connect(final InetSocketAddress inetSocketAddress) {
    return overlay.connect(inetSocketAddress).thenApply(ignored -> null);
  }


  public CompletionStage<Void> join(final InetSocketAddress inetSocketAddress) {
    return overlay.connect(inetSocketAddress).thenApply(peer -> {
      return null;
    });
  }

  public CompletionStage<Peer> lookup(final UUID uuid) {
//    for (final Peer peer : fingers) {
//
//    }
    return null;
  }

  private CompletionStage<Peer> getClosestPreceedingFinger(final UUID uuid) {
    for (final Peer peer : Lists.reverse(fingers)) {
      if (isBetween(localUUID, uuid, peer.getUUID())) {
        return completedFuture(peer);
      }
    }

    return null;
//    return getLocalPeer();
  }

  private boolean isBetween(final UUID from, final UUID to, final UUID uuid) {
    if (to.compareTo(from) > 0) {
      return uuid.compareTo(from) > 0 && uuid.compareTo(to) < 0;
    } else {
      return uuid.compareTo(from) > 0 || uuid.compareTo(to) < 0;
    }
  }


  @Override
  public void close() throws IOException {
    overlay.close();
  }
}
