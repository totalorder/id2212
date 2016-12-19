package org.deadlock.id2212;

import com.google.common.collect.Lists;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.messages.HeartbeatRequest;
import org.deadlock.id2212.messages.HeartbeatResponse;
import org.deadlock.id2212.messages.PredecessorNotification;
import org.deadlock.id2212.messages.PredecessorRequest;
import org.deadlock.id2212.messages.PredecessorResponse;
import org.deadlock.id2212.messages.SuccessorRequest;
import org.deadlock.id2212.messages.SuccessorResponse;
import org.deadlock.id2212.overlay.Overlay;
import org.deadlock.id2212.overlay.Peer;
import org.deadlock.id2212.overlay.PeerExchangeOverlay;

import javax.swing.text.html.StyleSheet;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
  private final int numFingers = 5;
  private int localUUID;
  private Peer predecessor;
  private Peer successor;
  private final Map<Integer, Peer> connectedPeers = new HashMap<>();
  private int nextFingerToFix;
  private final double K = 256;
  private ScheduledExecutorService scheduledExecutorService;
  private CompletableFuture<Void> isStable = new CompletableFuture<>();
  private CompletableFuture<Void> fingersFixed = new CompletableFuture<>();
  private Map<Peer, Instant> heartbeats = new HashMap<>();
  private boolean running = false;
  private CompletableFuture<Void> stopped;
  private CompletableFuture<Void> isAlone = new CompletableFuture<>();

  public DHT(final Overlay overlay) {
    overlay.setOnMessageReceivedCallback(this::onMessageReceived);
    overlay.setOnPeerAcceptedCallback(this::onPeerAccepted);
    overlay.setOnPeerConnectedCallback(this::onPeerConnected);

    overlay.registerType(HeartbeatRequest.class);
    overlay.registerType(HeartbeatResponse.class);
    overlay.registerType(PredecessorNotification.class);
    overlay.registerType(PredecessorRequest.class);
    overlay.registerType(PredecessorResponse.class);
    overlay.registerType(SuccessorRequest.class);
    overlay.registerType(SuccessorResponse.class);
    this.overlay = overlay;
    localUUID = overlay.getUUID();
  }

  private void onMessageReceived(final Peer peer, final IdJsonMessage message) {
    if (!running) {
      return;
    }
    System.out.println(localUUID + " received " + message);
    if (message.isClass(PredecessorRequest.class)) {
      peer.send(new PredecessorResponse(predecessor != null ? predecessor.getUUID() : null, overlay.getListeningAddress()), message.getUUID());
    } else if (message.isClass(SuccessorRequest.class)) {
      final SuccessorRequest successorRequest = message.getObject(SuccessorRequest.class);
      onSuccessorRequest(peer, message, successorRequest);
    } else if (message.isClass(PredecessorNotification.class)) {
      final PredecessorNotification predecessorNotification = message.getObject(PredecessorNotification.class);
      onPredecessorNotification(peer, message, predecessorNotification);
    } else  if (message.isClass(HeartbeatRequest.class)) {
      peer.send(new HeartbeatResponse(), message.getUUID());
    }
  }

  private void onPredecessorNotification(final Peer peer, final IdJsonMessage message, final PredecessorNotification predecessorNotification) {
    if (predecessor == null || isBetween(predecessorNotification.uuid, predecessor.getUUID(), localUUID)) {
      predecessor = peer;
    }
  }

  private void onSuccessorRequest(final Peer peer, final IdJsonMessage message, final SuccessorRequest successorRequest) {
    if (isBetween(successorRequest.uuid, localUUID, successor.getUUID())) {
      peer.send(new SuccessorResponse(localUUID, overlay.getListeningAddress()), message.getUUID());
    } else {
      getClosestPreceedingFinger(successorRequest.uuid)
          .send(successorRequest)
          .thenCompose(peer::receive)
          .thenCompose(reply ->
                  peer.send(reply, message.getUUID())
          );
    }
  }

  private CompletionStage<Peer> findSuccessor(int uuid) {
    if (isBetween(uuid, localUUID, successor.getUUID())) {
      return completedFuture(me);
    } else {
      final Peer closestFinger = getClosestPreceedingFinger(uuid);
      if (closestFinger == me) {
        return completedFuture(successor);
      }
      return closestFinger.send(new SuccessorRequest(uuid))
          .thenCompose(closestFinger::receive)
          .thenCompose(reply -> {
            final SuccessorResponse successorResponse = reply.getObject(SuccessorResponse.class);
            return getOrConnectPeer(successorResponse.successor, successorResponse.address);
          });
    }
  }

  private void onPeerAccepted(final Peer peer) {
    synchronized (connectedPeers) {
      connectedPeers.put(peer.getUUID(), peer);
    }
    System.out.println("Accepted connection from " + peer.getUUID());
  }

  private void onPeerConnected(final Peer peer) {
    System.out.println("Connected to " + peer.getUUID());
  }

  public static DHT createDefault() {
    return new DHT(PeerExchangeOverlay.createDefault());
  }

  public CompletionStage<Void> start(final int port) {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    return overlay.start(port).thenApply(ignored -> {
      System.out.println("UUID: " + overlay.getUUID());
      System.out.println("Listening on port " + port + "...");
      return null;
    });
  }

  public int getListeningPort() {
    return overlay.getListeningPort();
  }

//  public CompletionStage<Void> connect(final InetSocketAddress inetSocketAddress) {
//    return overlay.connect(inetSocketAddress).thenApply(ignored -> null);
//  }

  public CompletionStage<Void> join(final InetSocketAddress inetSocketAddress) {
    running = true;
    while (fingers.size() < numFingers) {
      fingers.add(me);
    }

    predecessor = null;

    stabilizeForever();

    return overlay.connect(inetSocketAddress)
        .thenApply(peer -> {
          synchronized (connectedPeers) {
            connectedPeers.put(peer.getUUID(), peer);
          }
          return peer;
        })
        .thenCompose(peer -> findSuccessor(peer, localUUID)
            .thenApply(successor -> {
              System.out.println(localUUID + " Found successor " + successor.getUUID());
              this.successor = successor;
              return null;
            }));
  }

  public CompletionStage<Void> waitForStable() {
    return isStable;
  }

  public CompletionStage<Void> waitForAlone() {
    isAlone = new CompletableFuture<>();
    return isAlone;
  }

  public CompletionStage<Void> waitForFingersFixed() {
    nextFingerToFix = 0;
    fingersFixed = new CompletableFuture<>();
    return fingersFixed;
  }

  public CompletionStage<Peer> findSuccessor(final Peer peer, final int uuid) {
//    System.out.println(localUUID + " findSuccessor at " + peer.getUUID() + " for " + uuid);
    return peer.send(new SuccessorRequest(uuid))
        .thenCompose(peer::receive)
        .thenCompose(reply -> {
          final SuccessorResponse successorResponse = reply.getObject(SuccessorResponse.class);
          return getOrConnectPeer(successorResponse.successor, successorResponse.address);
        });
  }

  private CompletionStage<Peer> getOrConnectPeer(final Integer uuid, final InetSocketAddress address) {
    if (uuid == null) {
      return completedFuture(null);
    }

    if (uuid == localUUID) {
      return completedFuture(me);
    }

    Peer peer;
    synchronized (connectedPeers) {
      peer = connectedPeers.get(uuid);
    }

    if (peer != null) {
      return completedFuture(peer);
    } else {
      System.out.println(localUUID + " peer " + uuid + " not found. Connecting");
      return overlay.connect(address)
        .thenApply(connectedPeer -> {
          synchronized (connectedPeers) {
            connectedPeers.put(connectedPeer.getUUID(), connectedPeer);
          }
          return connectedPeer;
      });
    }
  }

  public CompletionStage<Void> heartbeat() {
    if (predecessor != null && predecessor != me) {
      System.out.println(localUUID + " Sending heartbeat!");
      final CompletableFuture<IdJsonMessage> replyFuture = predecessor.send(new HeartbeatRequest()).thenCompose(predecessor::receive).toCompletableFuture();

      scheduledExecutorService.schedule(() -> {
        if (!replyFuture.isDone()) {
          replyFuture.completeExceptionally(new RuntimeException("Heartbeat time out!"));
          System.out.println(localUUID + " predecessor heartbeat timed out !");
          predecessor = null;
        } else {
          System.out.println(localUUID + " predecessor heartbeat received on time");
        }
        return null;
      }, 400, TimeUnit.MILLISECONDS);

      return replyFuture.thenApply(ignored -> null);
    } else {
      System.out.println(localUUID + " Sending NO heartbeat!");
    }
    return completedFuture(null);
  }

  public CompletionStage<Void> stabilize() {
    if (successor == me && predecessor == null && !isAlone.isDone()) {
      isAlone.complete(null);
    }

    if (successor == null) {
      return completedFuture(null);
    }

    if (successor == me && (predecessor == me || predecessor == null)) {
      return completedFuture(null);
    }

    if (successor == me) {
      return updateSuccessorAndSendNotification(predecessor);
    }

    if (predecessor != null && predecessor != me && !isStable.isDone()) {
      isStable.complete(null);
    }

    System.out.println(localUUID + " Sending predecessor request to " + successor.getUUID());
    return successor.send(new PredecessorRequest())
        .thenApply(uuid -> {
          System.out.println(localUUID + " Sent predecessor request");
          return uuid;
        })
        .thenCompose(successor::receive)
        .thenCompose(reply -> {
          final PredecessorResponse predecessorResponse = reply.getObject(PredecessorResponse.class);
          return getOrConnectPeer(predecessorResponse.predecessor, predecessorResponse.address)
              .thenCompose(this::updateSuccessorAndSendNotification);
        }).exceptionally(throwable -> {
          successor = null;
          return null;
        });
  }

  private CompletionStage<Void> updateSuccessorAndSendNotification(final Peer peer) {
    if (peer != null && isBetween(peer.getUUID(), localUUID, successor.getUUID())) {
      successor = peer;
    }
    return successor.send(new PredecessorNotification(localUUID, overlay.getListeningAddress()))
        .thenApply(ignored -> null);
  }

  public CompletionStage<Void> fixFingers() {
    if (successor == null) {
      return completedFuture(null);
    }
    if (nextFingerToFix > fingers.size() - 1) {
      if (!fingersFixed.isDone()) {
        fingersFixed.complete(null);
      }
      nextFingerToFix = 0;
    }

    final int nextPosition = localUUID + (int) Math.pow(K, nextFingerToFix);
    return findSuccessor(nextPosition).thenApply(successor -> {
      System.out.println(localUUID + " set finger " + nextFingerToFix + " to " + successor.getUUID() + " at position " + nextPosition);
      fingers.set(nextFingerToFix, successor);
      nextFingerToFix = nextFingerToFix + 1;
      return null;
    });
  }

  public void initiate() {
    running = true;
    predecessor = null;
    successor = me;
    while (fingers.size() < numFingers) {
      fingers.add(me);
    }

    stabilizeForever();
  }

  private Peer getClosestPreceedingFinger(final int uuid) {
    for (final Peer peer : Lists.reverse(fingers)) {
      if (isBetween(peer.getUUID(), localUUID, uuid)) {
        return peer;
      }
    }

    return me;
  }

  private boolean isBetween(final int uuid, final int from, final int to) {
    if (to == from) {
      return true;
    }
    if (to > from) {
      return uuid > from && uuid < to;
    } else {
      return uuid > from || uuid < to;
    }
  }

  public CompletableFuture<Void> stop() {
    stopped = new CompletableFuture<>();
    running = false;
    return stopped;
  }

  private void stabilizeForever() {
    // Announce known peers to all connected peers every second
    scheduledExecutorService.schedule(() -> {
      try {
        heartbeat().toCompletableFuture().get();
        stabilize().toCompletableFuture().get();
        fixFingers().toCompletableFuture().get();

      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        System.out.println(localUUID + " predecessor: " + (predecessor != null ? predecessor.getUUID() : "null"));
        System.out.println(localUUID + " successor: " + (successor != null ? successor.getUUID() : "null"));
        if (running) {
          stabilizeForever();
        } else {
          if (!stopped.isDone()) {
            stopped.complete(null);
          }
        }
      }
    }, 1, TimeUnit.SECONDS);
  }

  public void printFingerTable() {
    System.out.println(localUUID + " predecessor: " + (predecessor != null ? predecessor.getUUID() : "null"));
    System.out.println(localUUID + " successor: " + (successor != null ? successor.getUUID() : "null"));
    for (final Peer finger : fingers) {
          System.out.println(localUUID + " finger: " + finger.getUUID());
    }
  }

  @Override
  public void close() throws IOException {
    overlay.close();
  }

  private final Peer me = new Peer() {
    @Override
    public CompletionStage<UUID> send(final Object object) {
      throw new RuntimeException("This is me");
    }

    @Override
    public CompletionStage<UUID> send(final Object object, final UUID uuid) {
      throw new RuntimeException("This is me");
    }

    @Override
    public CompletionStage<IdJsonMessage> receive(final UUID uuid) {
      throw new RuntimeException("This is me");
    }

    @Override
    public int getUUID() {
      return localUUID;
    }

    @Override
    public void setOnMessageReceivedCallback(final Consumer<IdJsonMessage> callback) {
      throw new RuntimeException("This is me");
    }

    @Override
    public InetSocketAddress getAddress() {
      throw new RuntimeException("This is me");
    }
  };
}
