package org.deadlock.id2212.asyncio;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class AsyncQueue<T> {
  private final Queue<T> incoming = new LinkedList<>();
  private final Queue<CompletableFuture<T>> requested = new LinkedList<>();

  public AsyncQueue() {
  }

  private void update() {
    T element = null;
    CompletableFuture<T> future = null;
    synchronized (incoming) {
      synchronized (requested) {
        if (incoming.peek() != null && requested.peek() != null) {
          element = incoming.remove();
          future = requested.remove();
        }
      }
    }

    if (element != null) {
      future.complete(element);
    }
  }

  public void add(T element) {
    synchronized (incoming) {
      incoming.add(element);
    }
    update();
  }

  public CompletionStage<T> remove() {
    final CompletableFuture<T> future = new CompletableFuture<>();
    synchronized (requested) {
      requested.add(future);
    }
    update();
    return future;
  }
}
