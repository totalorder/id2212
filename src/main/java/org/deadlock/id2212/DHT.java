package org.deadlock.id2212;

import com.google.common.collect.Lists;
import org.deadlock.id2212.asyncio.AsyncIO;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.messages.HeartbeatRequest;
import org.deadlock.id2212.messages.HeartbeatResponse;
import org.deadlock.id2212.messages.PredecessorNotification;
import org.deadlock.id2212.messages.PredecessorRequest;
import org.deadlock.id2212.messages.PredecessorResponse;
import org.deadlock.id2212.messages.RingProbe;
import org.deadlock.id2212.messages.SuccessorRequest;
import org.deadlock.id2212.messages.SuccessorResponse;
import org.deadlock.id2212.overlay.Overlay;
import org.deadlock.id2212.overlay.Peer;
import org.deadlock.id2212.overlay.PeerExchangeOverlay;
import org.deadlock.id2212.util.Timeout;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.deadlock.id2212.util.CompletableExtras.async;

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
  private boolean running = false;
  private CompletableFuture<Void> stopped;
  private CompletableFuture<Void> isAlone = new CompletableFuture<>();
  private CompletableFuture<RingProbe> lastProbeReceieved = new CompletableFuture<>();
  private boolean logMessages = false;
  private Timeout timeout = new Timeout();

  public DHT(final Overlay overlay) {
    overlay.setOnMessageReceivedCallback(this::onMessageReceived);
    overlay.setOnPeerAcceptedCallback(this::onPeerAccepted);
    overlay.setOnPeerConnectedCallback(this::onPeerConnected);
    overlay.setOnPeerBrokenPipeCallback(this::onPeerBrokenPipe);

    overlay.registerType(HeartbeatRequest.class);
    overlay.registerType(HeartbeatResponse.class);
    overlay.registerType(PredecessorNotification.class);
    overlay.registerType(PredecessorRequest.class);
    overlay.registerType(PredecessorResponse.class);
    overlay.registerType(RingProbe.class);
    overlay.registerType(SuccessorRequest.class);
    overlay.registerType(SuccessorResponse.class);
    this.overlay = overlay;
    localUUID = overlay.getUUID();
  }

  private void onPeerBrokenPipe(final Peer peer) {
//    log("broken pipe: " + peer.getUUID());
    if (successor == peer) {
      successor = null;
    }

    if (predecessor == peer) {
      predecessor = null;
    }

    while (fingers.remove(peer)) {
      fingers.addFirst(me);
    }
    log("done breaking");
  }

  private void onMessageReceived(final Peer peer, final IdJsonMessage message) {
    if (!running) {
      return;
    }

    if (logMessages) {
      log("received " + message);
    }

//    System.out.println(localUUID + " received " + message);
    if (message.isClass(PredecessorRequest.class)) {
      if (predecessor != null) {
//        log("Sending predecessor response: %s, %s", predecessor.getUUID(), predecessor.getListeningAddress());
        peer.send(new PredecessorResponse(predecessor.getUUID(), predecessor.getListeningAddress()), message.getUUID());
      } else {
        peer.send(new PredecessorResponse(null, null), message.getUUID());
      }

    } else if (message.isClass(SuccessorRequest.class)) {
      final SuccessorRequest successorRequest = message.getObject(SuccessorRequest.class);
      onSuccessorRequest(peer, message, successorRequest);
    } else if (message.isClass(PredecessorNotification.class)) {
      final PredecessorNotification predecessorNotification = message.getObject(PredecessorNotification.class);
      onPredecessorNotification(peer, message, predecessorNotification);
    } else  if (message.isClass(HeartbeatRequest.class)) {
      peer.send(new HeartbeatResponse(), message.getUUID());
    } else if (message.isClass(RingProbe.class)) {
      onRingProbe(message.getObject(RingProbe.class));
    }
  }

  private void onRingProbe(final RingProbe ringProbe) {
    if (ringProbe.nodes.get(0) == localUUID) {
      lastProbeReceieved.complete(ringProbe);
      lastProbeReceieved = new CompletableFuture<>();
    } else if (successor != null && successor != me){
      ringProbe.nodes.add(localUUID);
      successor.send(ringProbe);
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
      final Peer closestFinger = getClosestPreceedingFinger(successorRequest.uuid);
      if (closestFinger != me) {
        closestFinger.send(successorRequest)
            .thenCompose(peer::receive)
            .thenCompose(reply ->
                    peer.send(reply, message.getUUID())
            );
      }
    }
  }

  private CompletionStage<Peer> findSuccessor(int uuid) {
    if (successor != null && isBetween(uuid, localUUID + 1, successor.getUUID())) {
      return completedFuture(me);
    } else {
      final Peer closestFinger = getClosestPreceedingFinger(uuid);
      if (closestFinger == me) {
        if (successor == null) {
          return completedFuture(me);
        }
        return completedFuture(successor);
      }

      return timeout.catchTimeout(closestFinger.send(new SuccessorRequest(uuid))
          .thenComposeAsync(closestFinger::receive)
          .thenCompose(reply -> {
            final SuccessorResponse successorResponse = reply.getObject(SuccessorResponse.class);
            return getOrConnectPeer(successorResponse.successor, successorResponse.address);
          }), () -> {
            fingers.remove(closestFinger);
            fingers.addFirst(me);
            return me;
          });
    }
  }

  private void onPeerAccepted(final Peer peer) {
    synchronized (connectedPeers) {
      connectedPeers.put(peer.getUUID(), peer);
    }
    log("Accepted connection from " + peer.getUUID());
  }

  private void onPeerConnected(final Peer peer) {
    synchronized (connectedPeers) {
      connectedPeers.put(peer.getUUID(), peer);
    }
    log("Connected to " + peer.getUUID());
  }

  public static DHT createDefault() {
    return new DHT(PeerExchangeOverlay.createDefault());
  }

  public static DHT createDefault(final int uuid) {
    return new DHT(PeerExchangeOverlay.createDefault(uuid));
  }

  public CompletionStage<Void> start(final int port) {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    return overlay.start(port).thenApply(ignored -> {
      log("Started on " + overlay.getListeningAddress());
//      System.out.println("UUID: " + overlay.getUUID());
//      System.out.println("Listening on port " + port + "...");
      return null;
    });
  }

  public InetSocketAddress getListeningAddress() {
    return overlay.getListeningAddress();
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

    return async(() -> {
      return overlay.connect(inetSocketAddress)
          .thenApply(peer -> {
            synchronized (connectedPeers) {
              connectedPeers.put(peer.getUUID(), peer);
            }
            return peer;
          })
          .thenCompose(peer -> findSuccessor(peer, localUUID)
              .thenApply(successor -> {
                log("Found successor " + successor.getUUID());
                this.successor = successor;
                return null;
              }));
    });
  }



  public CompletionStage<Void> waitForStable() {
    isStable = new CompletableFuture<>();
    return isStable;
  }

  public CompletionStage<Void> waitForAlone() {
    isAlone = new CompletableFuture<>();
    return async(() -> isAlone);
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
      log("peer " + uuid + " not found. Connecting...");
      return overlay.connect(address);
    }
  }

  public CompletionStage<Void> heartbeat() {
    return handleErrors(async(() -> {
//      log("heartbeat init!");
      if (predecessor != null && predecessor != me) {
//      System.out.println(localUUID + " Sending heartbeat!");
        final CompletableFuture<IdJsonMessage> replyFuture = predecessor.send(new HeartbeatRequest())
            .thenCompose(predecessor::receive)
            .toCompletableFuture();

        final CompletableFuture<Void> voidReplyFuture = replyFuture.<Void>thenApplyAsync(ignoredAgain -> null);
        return timeout.timeout(voidReplyFuture, 500).exceptionally(throwable -> {
          if (throwable instanceof Timeout.TimeoutException) {
            log("heartbeat timeout!");
            predecessor = null;
            return null;
          } else {
            throw new RuntimeException(throwable);
          }
        });
      } else {
        log("Sending NO heartbeat!");
      }
      return completedFuture(null);
    }), "heartbeat");
  }

  private void log(final String log, final Object... args) {
//    System.out.println(Thread.currentThread().getName() + " " + localUUID + " " + String.format(log, args));
    System.out.println(localUUID + " " + String.format(log, args));
  }

  public CompletionStage<RingProbe> probeRing() {
    return async(() -> {
      lastProbeReceieved = new CompletableFuture<>();
      probeRingUntilResponse();
      return lastProbeReceieved;
    });
  }

  private void probeRingUntilResponse() {
    if (successor != null && successor != me) {
      log("sending ring probe");
      final RingProbe ringProbe = new RingProbe();
      ringProbe.nodes.add(localUUID);

      final CompletionStage<RingProbe> replyFuture = successor.send(ringProbe)
          .thenCompose(ignored -> lastProbeReceieved);
      timeout.timeout(replyFuture, 1000, () -> {
//        log("ex %s", throwable);
//        if (throwable instanceof Timeout.TimeoutException || throwable.getCause() instanceof Timeout.TimeoutException) {
        probeRingUntilResponse();
//        } else {
//          log("y?");
//          throw new RuntimeException(throwable);
//        }
//        return null;
      });
    }
  }

//  private <T> CompletionStage<T> forkJoin(Supplier<CompletionStage<T>> supplier) {
//    ForkJoinPool.commonPool().submit(new ForkJoinTask<Void>() {
//
////    ForkJoinTask.adapt(asd -> null);
//    final CompletableFuture<CompletionStage<T>> done = new CompletableFuture<>();
////    CompletableFuture.supplyAsync(() -> {
////
////    })
//    ForkJoinPool.commonPool().submit(() -> {
//      done.complete(supplier.get());
//    });
//
//    return done.get().g;
//  }

  public CompletionStage<Void> stabilize() {
    return handleErrors(async(() -> {
      if (successor == me && predecessor == null && !isAlone.isDone()) {
        isAlone.complete(null);
      }

      if (successor == null) {
        return findSuccessor(localUUID).thenApplyAsync(peer -> {
          successor = peer;
          return null;
        });
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

//      log("Sending predecessor request to " + successor.getUUID());
//    return timeout.catchTimeout(successor.send(new PredecessorRequest())
      return successor.send(new PredecessorRequest())
          .thenApply(uuid -> {
//            log("Sent predecessor request");
            return uuid;
          })
          .thenCompose(successor::receive)
          .thenCompose(reply -> {
            final PredecessorResponse predecessorResponse = reply.getObject(PredecessorResponse.class);
            return getOrConnectPeer(predecessorResponse.predecessor, predecessorResponse.address)
                .thenCompose(this::updateSuccessorAndSendNotification);
          });
    }), "stabilize");
  }

  private <T> CompletionStage<T> handleErrors(final CompletionStage<T> stage, final String actionName) {
    return stage.exceptionally(throwable -> {
      if (throwable instanceof Timeout.TimeoutException || throwable.getCause() instanceof Timeout.TimeoutException) {
        log("%s timed out!", actionName);
        return null;
      } else if (throwable.getCause() instanceof AsyncIO.BrokenPipeException) {
        log("%s broke its pipe!", actionName);
        return null;
      } else {
        throw new RuntimeException(throwable);
      }
    });
  }

  private CompletionStage<Void> updateSuccessorAndSendNotification(final Peer successorsPredecessor) {
    if (successorsPredecessor != null && isBetween(successorsPredecessor.getUUID(), localUUID, successor.getUUID())) {
      successor = successorsPredecessor;
    }

//    log("Sending predecessor notification to " + successor.getUUID());
    return successor.send(new PredecessorNotification(localUUID, overlay.getListeningAddress()))
        .thenApplyAsync(ignored -> null);
  }

  public CompletionStage<Void> fixFingers() {
    return handleErrors(async(() -> {
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
    }), "fix fingers");
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
      if (isBetween(uuid, localUUID, peer.getUUID())) {
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
    log("stopping");
    return async(() -> {
      connectedPeers.entrySet().forEach(entry -> {
        try {
          entry.getValue().close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
      connectedPeers.clear();
      stopped = new CompletableFuture<>();
      running = false;
      return stopped;
    }).toCompletableFuture();
  }

  private void stabilizeForever() {
    // Announce known peers to all connected peers every second
    scheduledExecutorService.schedule(() -> {
      try {
        heartbeat().toCompletableFuture().get();
//        ForkJoinPool.commonPool().submit(ForkJoinTask.adapt())
        stabilize().toCompletableFuture().get(15000, TimeUnit.MILLISECONDS);
        fixFingers().toCompletableFuture().get();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        log("predecessor: " + (predecessor != null ? predecessor.getUUID() : "null"));
        log("successor: " + (successor != null ? successor.getUUID() : "null"));
        if (running) {
//          log("stabilize forever");
          stabilizeForever();
        } else {
//          log("stabilize stop");
          if (!stopped.isDone()) {
            stopped.complete(null);
          }
        }
      }
    }, 1, TimeUnit.SECONDS);
  }

  public void printFingerTable() {
    log("predecessor: " + (predecessor != null ? predecessor.getUUID() : "null"));
    log("successor: " + (successor != null ? successor.getUUID() : "null"));
    for (final Peer finger : fingers) {
          System.out.println(localUUID + " finger: " + finger.getUUID());
    }
  }

  public int getUUID() {
    return localUUID;
  }

  @Override
  public void close() throws IOException {
    overlay.close();
  }

  private final Peer me = new Peer() {
    @Override
    public void close() throws IOException {
    }

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
    public InetSocketAddress getListeningAddress() {
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
    public void setOnBrokenPipeCallback(final Runnable callback) {
    }

    @Override
    public InetSocketAddress getAddress() {
      throw new RuntimeException("This is me");
    }

    @Override
    public void onBrokenPipe() {
    }
  };
}
