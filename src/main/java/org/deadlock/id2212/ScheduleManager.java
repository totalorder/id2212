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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ScheduleManager implements Closeable {
  private final Overlay overlay;
  private Schedule mySchedule = new Schedule();

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
    if (message.isClass(RequestSchedule.class)) {
      peer.send(mySchedule);
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

  private CompletionStage<Schedule> receiveNextSchedule(final Peer peer, final IdJsonMessage message) {
    if (message.isClass(Schedule.class)) {
      return completedFuture(message.getObject(Schedule.class));
    }
    return peer.receive().thenCompose(m -> receiveNextSchedule(peer, m));
  }

  public CompletionStage<Instant> findTime(final int otherPeers) {
    final CompletionStage<List<CompletionStage<Peer>>> broadCastedToPeersFutures =
        overlay.waitForConnectedPeers(otherPeers)
            .thenApply(ignoredToo ->
                    overlay.broadcast(new RequestSchedule())
            );

    return broadCastedToPeersFutures.thenCompose(peerFutures -> {
      final CompletionStage<List<Schedule>> scheduleFutures = allOfList(peerFutures
          .stream()
          .map(peerFuture ->
                  peerFuture.thenCompose((Peer peer) -> {
                    return peer.receive().thenCompose((IdJsonMessage message) -> {
                      return receiveNextSchedule(peer, message);
                    });
                  })
          ).collect(Collectors.toList()));

      return scheduleFutures.thenApply(schedules -> {
        if (schedules.size() == 0) {
          throw new NoMatchingTimeException("No peers connected");
        }

        final Set<Instant> matchingTimes = new HashSet<>();
        matchingTimes.addAll(mySchedule.availableTimes);
        schedules
            .stream()
            .forEach((Schedule schedule) ->
                matchingTimes.retainAll(schedule.availableTimes));
        if (matchingTimes.size() == 0) {
          throw new NoMatchingTimeException("No match found among " + schedules.size() + " other peers.");
        }
        return matchingTimes.iterator().next();
      });
    });
  }

  @Override
  public void close() throws IOException {
    overlay.close();
  }
}
