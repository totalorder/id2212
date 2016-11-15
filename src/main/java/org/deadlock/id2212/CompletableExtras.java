package org.deadlock.id2212;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
}
