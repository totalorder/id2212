package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.messages.RequestSchedule;
import org.deadlock.id2212.overlay.Overlay;
import org.deadlock.id2212.messages.Schedule;
import org.deadlock.id2212.overlay.Peer;
import static org.deadlock.id2212.CompletableExtras.allOfList;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;


public class ScheduleManager implements Closeable {
  private final Overlay overlay;
  private Schedule mySchedule = new Schedule();

  public ScheduleManager(final Overlay overlay) {
    overlay.setOnMessageReceivedCallback(this::onMessageReceived);
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

  public CompletionStage<Void> start(final int port) {
    return overlay.start(port);
  }

  public void addTime(final Instant time) {
    mySchedule.availableTimes.add(time);
  }

  public int getListeningPort() {
    return overlay.getListeningPort();
  }

  public CompletionStage<Void> connect(InetSocketAddress inetSocketAddress) {
    return overlay.connect(inetSocketAddress).thenApply(ignored -> null);
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
                    System.out.println("Receiving...");
                    return peer.receive().thenApply((IdJsonMessage message) ->
                        message.getObject(Schedule.class));
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
            .map((Schedule schedule) -> matchingTimes.retainAll(schedule.availableTimes));
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
