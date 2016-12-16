package org.deadlock.id2212;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Main {
  public static void main(String args[]) {
//    final ScheduleManager scheduleManager = ScheduleManager.createDefault();
//    try {
      if (args.length != 1 && args.length != 2 && args.length != 4) {
        System.out.println("Usage: schedule-file [listen-port] [connect-to-ip] [connect-to-port]");
        return;
      }

      final String scheduleFile = args[0];

      final int listenPort = Integer.parseInt(args[1]);
      try {
        final List<Instant> schedule = Files.lines(Paths.get(scheduleFile))
            .map(line -> LocalDateTime.parse(line).toInstant(ZoneOffset.UTC))
            .collect(Collectors.toList());
//        schedule.forEach(scheduleManager::addTime);
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }

      if (args.length == 4) {
        final String connectToIp = args[2];
        final int connectToPort = Integer.parseInt(args[3]);

//        scheduleManager.start(listenPort).toCompletableFuture().get();
//        scheduleManager.connect(new InetSocketAddress(connectToIp, connectToPort)).toCompletableFuture().get();
      } else if (args.length == 2) {
//        scheduleManager.start(listenPort).toCompletableFuture().get();
      } else {
//        scheduleManager.start(5000).toCompletableFuture().get();
      }

//      final Instant scheduledTime = scheduleManager.findTime(2).toCompletableFuture().get();
//      System.out.println("Found time! " + scheduledTime);
//      Thread.sleep(5000);
//    } catch (InterruptedException | ExecutionException e) {
//      if (e.getCause() instanceof NoMatchingTimeException) {
//        System.out.println("Error: " + e.getCause().getMessage());
//      } else {
//        e.printStackTrace();
//      }
//    }
  }
}
