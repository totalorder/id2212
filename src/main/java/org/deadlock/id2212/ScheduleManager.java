package org.deadlock.id2212;

import org.deadlock.id2212.messages.Schedule;

import java.util.concurrent.CompletionStage;

public class ScheduleManager {
  private final Overlay overlay;
  private int port;
  private Schedule mySchedule;

  public ScheduleManager(final Overlay overlay) {
    this.overlay = overlay;
  }

  public CompletionStage<String> findTime(final int port, final Schedule mySchedule) {
    this.port = port;
    this.mySchedule = mySchedule;
    return overlay.listen(port).thenCompose(ignored -> {
      return acceptUntilFoundTime();
    });
  }

  private CompletionStage<String> acceptUntilFoundTime() {
//    return overlay.accept().thenCompose(peer -> {
//      peer.send(mySchedule);
//    })
    return null;
  }
}
