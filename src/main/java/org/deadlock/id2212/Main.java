package org.deadlock.id2212;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class Main {
  public static void main(String args[]) {
      final DHT dht = DHT.createDefault();
//    try {
      if (args.length != 1 && args.length != 3) {
        System.out.println("Usage: <listen-port> [connect-to-ip] [connect-to-port]");
        return;
      }

      final int listenPort = Integer.parseInt(args[0]);
      dht.start(listenPort).toCompletableFuture().join();

      if (args.length == 3) {
        final String connectToIp = args[1];
        final int connectToPort = Integer.parseInt(args[2]);
        dht.join(new InetSocketAddress(connectToIp, connectToPort)).toCompletableFuture().join();
      } else if (args.length == 1) {
        dht.initiate();
      }

      final Scanner in = new Scanner(System.in);
      while (true) {
        try {
        final String command = in.nextLine();
          if (command.equals("probe")) {
            System.out.println(dht.probeRing().toCompletableFuture().get(3000, TimeUnit.MILLISECONDS));
          } else if (command.startsWith("put")) {
            final String[] tokens = command.split("\\s");
            if (tokens.length != 3) {
              System.out.println("Usage: put <key> <value>");
              continue;
            }

            dht.set(tokens[1], tokens[2]).toCompletableFuture().get(3000, TimeUnit.MILLISECONDS);
          } else if (command.startsWith("get")) {
            final String[] tokens = command.split("\\s");
            if (tokens.length != 2) {
              System.out.println("Usage: get <key>");
              continue;
            }
            System.out.println(dht.get(tokens[1]).toCompletableFuture().get(3000, TimeUnit.MILLISECONDS));
          }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          e.printStackTrace();
        }
      }
  }
}
