package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.messages.RequestSchedule;
import org.deadlock.id2212.overlay.Overlay;
import org.deadlock.id2212.messages.Schedule;
import org.deadlock.id2212.overlay.Peer;
import org.deadlock.id2212.overlay.PeerExchangeOverlay;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.deadlock.id2212.util.CompletableExtras.allOfList;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Find a common time among N peers in schedule X
 *
 * Automatically discovers other peers in the overlay after connecting
 * to a bootstrap node.
 */
public class ScheduleManager implements Closeable {
  private final Overlay overlay;
  private Schedule mySchedule = new Schedule();
  private HashMap<Peer, CompletableFuture<Schedule>> waitingForSchedules = new HashMap<>();

  public ScheduleManager(final Overlay overlay) {
    overlay.setOnMessageReceivedCallback(this::onMessageReceived);
    overlay.setOnPeerAcceptedCallback(this::onPeerAccepted);
    overlay.setOnPeerConnectedCallback(this::onPeerConnected);
    mySchedule.availableTimes = new LinkedList<>();
    overlay.registerType(Schedule.class);
    overlay.registerType(RequestSchedule.class);
    this.overlay = overlay;
  }

  private void onMessageReceived(final Peer peer, final IdJsonMessage message) {
    // Respond with own schedule to RequestSchedule messages
    if (message.isClass(RequestSchedule.class)) {
      peer.send(mySchedule);
    } else if (message.isClass(Schedule.class)) {
      final CompletableFuture<Schedule> scheduleFuture = waitingForSchedules.getOrDefault(peer, new CompletableFuture<>());
      scheduleFuture.complete(message.getObject(Schedule.class));
    }
  }

  private void onPeerAccepted(final Peer peer) {
    System.out.println("Accepted connection from " + peer.getUUID().toString());
  }

  private void onPeerConnected(final Peer peer) {
    System.out.println("Connected to " + peer.getUUID().toString());
  }

  public static ScheduleManager createDefault() {
    return new ScheduleManager(PeerExchangeOverlay.createDefault());
  }

  public CompletionStage<Void> start(final int port) {
    return overlay.start(port).thenApply(ignored -> {
      System.out.println("UUID: " + overlay.getUUID());
      System.out.println("Listening on port " + port + "...");
      return null;
    });
  }

  public void addTime(final Instant time) {
    System.out.println("Added time to schedule: " + LocalDateTime.ofInstant(time, ZoneOffset.UTC));
    mySchedule.availableTimes.add(time);
  }

  public int getListeningPort() {
    return overlay.getListeningPort();
  }

  public CompletionStage<Void> connect(InetSocketAddress inetSocketAddress) {
    return overlay.connect(inetSocketAddress).thenApply(ignored -> null);
  }

  /**
   * 1. Wait for N peers to be connected
   * 2. Broadcast a RequestSchedule message to all of them
   * 3. Receive Schedule message from all of them
   * 4. Find the common times among all schedules and return one
   */
  public CompletionStage<Instant> findTime(final int otherPeers) {
    // Broadcast RequestSchedule-message
    final CompletionStage<List<CompletionStage<Peer>>> broadCastedToPeersFutures =
        overlay.waitForConnectedPeers(otherPeers)
            .thenApply(ignoredToo -> {
              waitingForSchedules = new HashMap<>();
              return overlay.broadcast(new RequestSchedule());
            });

    return broadCastedToPeersFutures.thenCompose(peerFutures -> {
      // Receive Schedule messages from all peers
      final CompletionStage<List<Schedule>> scheduleFutures = allOfList(peerFutures
          .stream()
          .map(peerFuture ->
                  // TODO: Sync
                  peerFuture.thenCompose((Peer peer) -> waitingForSchedules.getOrDefault(peer, new CompletableFuture<>()))
          ).collect(Collectors.toList()));

      return scheduleFutures.thenApply(schedules -> {
        if (schedules.size() == 0) {
          throw new NoMatchingTimeException("No peers connected");
        }

        // Find the intersection of all schedules
        final Set<Instant> matchingTimes = new HashSet<>();
        matchingTimes.addAll(mySchedule.availableTimes);
        schedules
            .stream()
            .forEach((Schedule schedule) ->
                matchingTimes.retainAll(schedule.availableTimes));
        if (matchingTimes.size() == 0) {
          throw new NoMatchingTimeException("No match found among " + schedules.size() + " other peers.");
        }

        // Return the first one
        return matchingTimes.iterator().next();
      });
    });
  }

  @Override
  public void close() throws IOException {
    overlay.close();
  }
}
