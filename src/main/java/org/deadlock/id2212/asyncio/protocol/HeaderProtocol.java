package org.deadlock.id2212.asyncio.protocol;

import org.deadlock.id2212.asyncio.AsyncQueue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * Enable sending and receiving messages with a header of an arbitrary type. The subclasses
 * must implement serializing and deserializing of the header.
 */
public abstract class HeaderProtocol<ClientInterface> implements Protocol<ClientInterface> {
  private final MessageLengthProtocol messageLengthProtocol;

  public abstract static class HeaderProtocolClient<Header> implements HeaderClient<Header> {
    private final BytesClient bytesClient;
    private final AsyncQueue<HeadedMessage<Header>> receivedMessages = new AsyncQueue<>();
    private Consumer<HeadedMessage<Header>> onMessageReceivedCallback;
    private List<HeadedMessage<Header>> callbackQueue = new ArrayList<>();

    public HeaderProtocolClient(
        final BytesClient bytesClient) {
      this.bytesClient = bytesClient;
      bytesClient.setOnMessageReceivedCallback(this::onMessageReceived);
    }

    @Override
    public CompletionStage<Void> send(final Header header, byte[] bytes) {
      // Send the serialized data
      final byte[] headerData = serializeHeader(header);
      return bytesClient.send(
          headerData,
          bytes
      );
    }

    @Override
    public void setOnMessageReceivedCallback(Consumer<HeadedMessage<Header>> callback) {
      onMessageReceivedCallback = callback;
      ArrayList<HeadedMessage<Header>> callbackQueueCopy;
      synchronized (callbackQueue) {
        callbackQueueCopy = new ArrayList<>(callbackQueue);
        callbackQueue.clear();
      }
      callbackQueueCopy.stream().forEach(callback::accept);
    }

    abstract byte[] serializeHeader(final Header header);
    abstract HeadedMessage<Header> deserializeHeader(final byte[] bytes);

    public void onMessageReceived(final byte[] message) {
      final HeadedMessage<Header> headedMessage = deserializeHeader(message);
      if (onMessageReceivedCallback != null) {
        onMessageReceivedCallback.accept(headedMessage);
      } else {
        synchronized (callbackQueue) {
          callbackQueue.add(headedMessage);
        }
      }
      receivedMessages.add(headedMessage);
    }

    public CompletionStage<HeadedMessage<Header>> receive() {
      // Return the deserialized data
      return receivedMessages.remove();
    }

    @Override
    public InetSocketAddress getAddress() {
      return bytesClient.getAddress();
    }
  }

  public HeaderProtocol(final MessageLengthProtocol messageLengthProtocol) {
    this.messageLengthProtocol = messageLengthProtocol;
  }

  @Override
  public CompletionStage<ClientInterface> accept() {
    return messageLengthProtocol.accept().thenApply(this::createClient);
  }

  abstract ClientInterface createClient(final BytesClient bytesClient);

  @Override
  public CompletionStage<Void> startServer(final int port) {
    return messageLengthProtocol.startServer(port);
  }

  @Override
  public int getListeningPort() {
    return messageLengthProtocol.getListeningPort();
  }

  @Override
  public CompletionStage<ClientInterface> connect(final InetSocketAddress inetSocketAddress) {
    return messageLengthProtocol.connect(inetSocketAddress).thenApply(this::createClient);
  }

  @Override
  public void close() throws IOException {
    messageLengthProtocol.close();
  }
}
