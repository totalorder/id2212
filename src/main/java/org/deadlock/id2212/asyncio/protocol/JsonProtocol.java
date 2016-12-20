package org.deadlock.id2212.asyncio.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.deadlock.id2212.asyncio.AsyncQueue;
import org.deadlock.id2212.util.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Enables sending and receiving objects using JSON. Classes must be registered
 * using registerType() before sending/receiving.
 * Objects must be de/serializable with Jackson.
 */
public class JsonProtocol implements Protocol<IdJsonClient> {
  private final IntegerHeaderProtocol integerHeaderProtocol;
  private final ObjectMapper mapper;
  private final Map<Integer, Class> typeToClass = new HashMap<>();
  private final Map<Class, Integer> classToType = new HashMap<>();
  private int nextTypeId = 0;
  private Timeout timeout = new Timeout();

  public JsonProtocol(final IntegerHeaderProtocol integerHeaderProtocol) {
    this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    this.integerHeaderProtocol = integerHeaderProtocol;
  }

  public int registerType(final Class clazz) {
    nextTypeId++;
    typeToClass.put(nextTypeId, clazz);
    classToType.put(clazz, nextTypeId);
    return nextTypeId;
  }

  public static JsonProtocol createDefault() {
    return new JsonProtocol(IntegerHeaderProtocol.createDefault());
  }

  class JsonProtocolClient implements IdJsonClient {

    private final IntegerHeaderClient integerHeaderClient;
    private Consumer<IdJsonMessage> onMessageReceivedCallback;
    private AsyncQueue<IdJsonMessage> receivedMessages = new AsyncQueue<>();
    private List<IdJsonMessage> callbackQueue = new ArrayList<>();
    private final Map<UUID, CompletableFuture<IdJsonMessage>> waitingForReplies = new HashMap<>();
    private Runnable onBrokenPipeCallback;

    public JsonProtocolClient(final IntegerHeaderClient integerHeaderClient) {
      this.integerHeaderClient = integerHeaderClient;
      integerHeaderClient.setOnMessageReceivedCallback(this::onMessageReceived);
      integerHeaderClient.setOnBrokenPipeCallback(this::onBrokenPipe);
    }

    public void onMessageReceived(final HeadedMessage<IntegerUUIDHeader> message) {

      final IdJsonMessage jsonMessage = new IdJsonMessage(mapper, typeToClass, message.getHeader(), message.getBytes());
      receivedMessages.add(jsonMessage);

      CompletableFuture<IdJsonMessage> waiting;
      synchronized (waitingForReplies) {
        waiting = waitingForReplies.remove(jsonMessage.getUUID());
      }

      if (waiting != null) {
//        System.out.println("Received " + jsonMessage);
        waiting.complete(jsonMessage);
        return;
      }

      if (onMessageReceivedCallback != null) {
        onMessageReceivedCallback.accept(jsonMessage);
      } else {
        synchronized (callbackQueue) {
          callbackQueue.add(jsonMessage);
        }
      }
    }

    @Override
    public CompletionStage<UUID> send(final Object serializable) {
      return send(serializable, UUID.randomUUID());
    }

    @Override
    public CompletionStage<UUID> send(final Object serializable, final UUID uuid) {
      final Integer type = classToType.get(serializable.getClass());
      if (type == null) {
        throw new RuntimeException("Type for class " + serializable.getClass().getName() + " does not exists");
      }
      try {
        return integerHeaderClient.send(new IntegerUUIDHeader(type, uuid), mapper.writeValueAsBytes(serializable))
            .thenApply(ignored -> uuid);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public CompletionStage<IdJsonMessage> receive(final UUID uuid) {
      synchronized (waitingForReplies) {
        waitingForReplies.putIfAbsent(uuid, new CompletableFuture<>());
        final CompletableFuture<IdJsonMessage> messageFuture = waitingForReplies.get(uuid);
        return timeout.timeout(messageFuture, 500);
      }
    }

    public CompletionStage<IdJsonMessage> receive() {
      return receivedMessages.remove();
    }

    @Override
    public void setOnMessageReceivedCallback(Consumer<IdJsonMessage> callback) {
      onMessageReceivedCallback = callback;

      ArrayList<IdJsonMessage> callbackQueueCopy;
      synchronized (callbackQueue) {
        callbackQueueCopy = new ArrayList<>(callbackQueue);
        callbackQueue.clear();
      }
      callbackQueueCopy.stream().forEach(callback::accept);
    }

    @Override
    public void setOnBrokenPipeCallback(final Runnable callback) {
      onBrokenPipeCallback = callback;
    }

    @Override
    public void onBrokenPipe() {
      if (onBrokenPipeCallback != null) {
        onBrokenPipeCallback.run();
      }
    }

    @Override
    public InetSocketAddress getAddress() {
      return integerHeaderClient.getAddress();
    }

    @Override
    public void close() throws IOException {
      integerHeaderClient.close();
    }
  }

  @Override
  public CompletionStage<IdJsonClient> accept() {
    return integerHeaderProtocol.accept().thenApply(JsonProtocolClient::new);
  }

  @Override
  public CompletionStage<Void> startServer(int port) {
    return integerHeaderProtocol.startServer(port);
  }

  @Override
  public int getListeningPort() {
    return integerHeaderProtocol.getListeningPort();
  }

  @Override
  public InetSocketAddress getListeningAddress() {
    return integerHeaderProtocol.getListeningAddress();
  }

  @Override
  public CompletionStage<IdJsonClient> connect(InetSocketAddress inetSocketAddress) {
    return integerHeaderProtocol.connect(inetSocketAddress).thenApply(JsonProtocolClient::new);
  }

  @Override
  public void close() throws IOException {
    integerHeaderProtocol.close();
  }
}
