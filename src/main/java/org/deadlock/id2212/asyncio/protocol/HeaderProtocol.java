package org.deadlock.id2212.asyncio.protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

public abstract class HeaderProtocol<ClientInterface> implements Protocol<ClientInterface> {
  private final MessageLengthProtocol messageLengthProtocol;

  public abstract static class HeaderProtocolClient<Header> implements HeaderClient<Header> {
    private final BytesClient bytesClient;

    public HeaderProtocolClient(
        final BytesClient bytesClient) {
      this.bytesClient = bytesClient;
    }

    @Override
    public CompletionStage<Void> send(final Header header, byte[] bytes) {
      final byte[] headerData = serializeHeader(header);
      return bytesClient.send(
          headerData,
          bytes
      );
    }

    abstract byte[] serializeHeader(final Header header);
    abstract HeadedMessage<Header> deserializeHeader(final byte[] bytes);

    @Override
    public CompletionStage<HeadedMessage<Header>> receive() {
      return bytesClient.receive().thenApply(this::deserializeHeader);
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
