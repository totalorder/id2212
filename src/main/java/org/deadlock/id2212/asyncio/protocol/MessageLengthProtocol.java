package org.deadlock.id2212.asyncio.protocol;

import org.deadlock.id2212.asyncio.AsyncQueue;
import org.deadlock.id2212.asyncio.AsyncIO;
import org.deadlock.id2212.asyncio.AsyncIOClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class MessageLengthProtocol implements Protocol<BytesClient> {
  public static class MessageLengthProtocolClient implements BytesClient {
    private final AsyncIOClient asyncIOClient;
    private final AsyncQueue<ByteBuffer> receivedMessages = new AsyncQueue<>();
    private ByteBuffer lengthBuffer = ByteBuffer.allocateDirect(4);
    private ByteBuffer messageBuffer = null;
    private int messageLength;

    public MessageLengthProtocolClient(AsyncIOClient asyncIOClient) {
      this.asyncIOClient = asyncIOClient;
    }

    public void onDataReceived(final ByteBuffer buffer) {
      if (messageBuffer == null) {
        while (lengthBuffer.hasRemaining() && buffer.hasRemaining()) {
          lengthBuffer.put(buffer.get());
        }

        if (!lengthBuffer.hasRemaining()) {
          lengthBuffer.flip();
          messageLength = lengthBuffer.getInt();
          lengthBuffer.clear();
          messageBuffer = ByteBuffer.allocateDirect(messageLength);
        }
      } else {
        while (messageBuffer.hasRemaining() && buffer.hasRemaining()) {
          messageBuffer.put(buffer.get());
        }

        if (!messageBuffer.hasRemaining()) {
          messageBuffer.flip();
          receivedMessages.add(messageBuffer);
          messageBuffer = null;
        }
      }

      if (buffer.hasRemaining()) {
        onDataReceived(buffer);
      }
    }

    @Override
    public CompletionStage<Void> send(final byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.allocateDirect(4 + bytes.length);
      buffer.putInt(bytes.length);
      buffer.put(bytes);
      buffer.flip();
      return asyncIOClient.send(buffer);
    }

    @Override
    public CompletionStage<byte[]> receieve() {
      return receivedMessages.remove().thenApply(buffer -> {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
      });
    }
  }

  private final AsyncIO asyncIO;
  private final AsyncQueue<AsyncIOClient> acceptedClients = new AsyncQueue<>();
  private final Map<AsyncIOClient, MessageLengthProtocolClient> clientMap = new HashMap<>();

  public MessageLengthProtocol(final AsyncIO asyncIO) {
    this.asyncIO = asyncIO;
    asyncIO.setClientDataReceivedCallback(this::onClientDataReceived);
  }

  @Override
  public CompletionStage<BytesClient> accept() {
    return acceptedClients.remove().thenApply(this::createClient);
  }

  private BytesClient createClient(AsyncIOClient asyncIOClient) {
    final MessageLengthProtocolClient client = new MessageLengthProtocolClient(asyncIOClient);
    clientMap.put(asyncIOClient, client);
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

  @Override
  public void close() throws IOException {
    asyncIO.close();
  }
}
