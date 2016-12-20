package org.deadlock.id2212.asyncio.protocol;

import org.deadlock.id2212.asyncio.AsyncQueue;
import org.deadlock.id2212.asyncio.AsyncIO;
import org.deadlock.id2212.asyncio.AsyncIOClient;
import org.deadlock.id2212.asyncio.TCPAsyncIO;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Enable sending and receiving messages of a certain length
 */
public class MessageLengthProtocol implements Protocol<BytesClient> {
  public static MessageLengthProtocol createDefault() {
    return new MessageLengthProtocol(new TCPAsyncIO());
  }

  public static class MessageLengthProtocolClient implements BytesClient {
    private final AsyncIOClient asyncIOClient;
    private Consumer<byte[]> onMessageReceivedCallback;
    private final AsyncQueue<byte[]> receivedMessages = new AsyncQueue<>();
    private ByteBuffer lengthBuffer = ByteBuffer.allocateDirect(4);
    private ByteBuffer messageBuffer = null;
    private int messageLength;
    private List<byte[]> callbackQueue = new ArrayList<>();
    private Runnable onBrokenPipeCallback;

    public MessageLengthProtocolClient(final AsyncIOClient asyncIOClient) {
      this.asyncIOClient = asyncIOClient;
    }

    /**
     * Read the message length, then read that many bytes, add message to
     * received message buffer and then start over.
     */
    public void onDataReceived(final ByteBuffer buffer) {
      // No message buffer, read length
      if (messageBuffer == null) {
        // Read the message length into lengthBuffer
        while (lengthBuffer.hasRemaining() && buffer.hasRemaining()) {
          lengthBuffer.put(buffer.get());
        }

        // If lengthBuffer is read, decode message length, store it in messageLength and
        // allocate message buffer
        if (!lengthBuffer.hasRemaining()) {
          lengthBuffer.flip();
          messageLength = lengthBuffer.getInt();
          lengthBuffer.clear();
          messageBuffer = ByteBuffer.allocateDirect(messageLength);
        }
      } else {
        // Read until message buffer is full. Put on queue when done
        while (messageBuffer.hasRemaining() && buffer.hasRemaining()) {
          messageBuffer.put(buffer.get());
        }

        if (!messageBuffer.hasRemaining()) {
          messageBuffer.flip();

          byte[] bytes = new byte[messageBuffer.remaining()];
          messageBuffer.get(bytes);
          receivedMessages.add(bytes);
          if (onMessageReceivedCallback != null) {
            onMessageReceivedCallback.accept(bytes);
          } else {
            synchronized (callbackQueue) {
              callbackQueue.add(bytes);
            }
          }

          messageBuffer = null;
        }
      }

      if (buffer.hasRemaining()) {
        onDataReceived(buffer);
      }
    }

    /**
     * Send a number of byte-chunks in a message with the total size as a header
     */
    @Override
    public CompletionStage<Void> send(final byte[]... byteArrays) {
      int totalSize = 0;
      for (byte[] bytes : byteArrays) {
        totalSize += bytes.length;
      }

      // Write the total size of all chunks as a 32-bit integer at start of message
      final ByteBuffer buffer = ByteBuffer.allocateDirect(4 + totalSize);
      buffer.putInt(totalSize);
      for (byte[] bytes : byteArrays) {
        buffer.put(bytes);
      }

      buffer.flip();
      return asyncIOClient.send(buffer);
    }

    public CompletionStage<byte[]> receive() {
      return receivedMessages.remove().thenApply(buffer -> {
//        byte[] bytes = new byte[buffer.remaining()];
//        buffer.get(bytes);
//        return bytes;
        return buffer;
      });
    }

    @Override
    public InetSocketAddress getAddress() {
      return asyncIOClient.getAddress();
    }

    @Override
    public void setOnMessageReceivedCallback(Consumer<byte[]> callback) {
      onMessageReceivedCallback = callback;
      ArrayList<byte[]> callbackQueueCopy;
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
    public void close() throws IOException {
      asyncIOClient.close();
    }
  }

  private final AsyncIO asyncIO;
  private final AsyncQueue<AsyncIOClient> acceptedClients = new AsyncQueue<>();
  private final Map<AsyncIOClient, MessageLengthProtocolClient> clientMap = new HashMap<>();

  public MessageLengthProtocol(final AsyncIO asyncIO) {
    this.asyncIO = asyncIO;
    asyncIO.setClientDataReceivedCallback(this::onClientDataReceived);
    asyncIO.setClientBrokenPipeCallback(this::onClientBrokenPipe);
  }

  @Override
  public CompletionStage<BytesClient> accept() {
    return acceptedClients.remove().thenApply(this::createClient);
  }

  private BytesClient createClient(AsyncIOClient asyncIOClient) {
    final MessageLengthProtocolClient client = new MessageLengthProtocolClient(asyncIOClient);
    clientMap.put(asyncIOClient, client);
    asyncIOClient.readyToReadAndWrite();
    return client;
  }

  @Override
  public CompletionStage<Void> startServer(final int port) {
    return asyncIO.startServer(port, this::onClientAccepted);
  }

  @Override
  public int getListeningPort() {
    return asyncIO.getListeningPort();
  }

  @Override
  public InetSocketAddress getListeningAddress() {
    return asyncIO.getListeningAddress();
  }

  @Override
  public CompletionStage<BytesClient> connect(final InetSocketAddress inetSocketAddress) {
    return asyncIO.connect(inetSocketAddress).thenApply(this::createClient);
  }

  private void onClientAccepted(final AsyncIOClient asyncIOClient) {
    acceptedClients.add(asyncIOClient);
  }

  private void onClientDataReceived(final AsyncIOClient asyncIOClient,
                                    final ByteBuffer buffer) {
    clientMap.get(asyncIOClient).onDataReceived(buffer);
  }

  private void onClientBrokenPipe(final AsyncIOClient asyncIOClient) {
    clientMap.get(asyncIOClient).onBrokenPipe();
  }

  @Override
  public void close() throws IOException {
    asyncIO.close();
  }
}
