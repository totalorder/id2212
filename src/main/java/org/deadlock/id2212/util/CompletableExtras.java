package org.deadlock.id2212.util;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CompletableExtras {
  public static <T> CompletionStage<List<T>> allOfList(final List<CompletionStage<T>> futures) {
    return CompletableFuture.allOf(
        futures.toArray(new CompletableFuture[futures.size()])).thenApply(ignored ->
            futures
                .stream()
                .map(CompletionStage::toCompletableFuture)
                .map(CompletableFuture::join).collect(Collectors.toList())
    );
  }

  public static <T> CompletionStage<T> async(Supplier<CompletionStage<T>> supplier) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.complete(null);

    return future.<T>thenComposeAsync(ignored ->
            supplier.get()
    );
  }

  public static <T> CompletionStage<T> asyncApply(Supplier<T> supplier) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.complete(null);

    return future.thenApplyAsync(ignored ->
            supplier.get()
    );
  }
}
