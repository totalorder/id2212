package org.deadlock.id2212.asyncio.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

public class JsonProtocol implements Protocol<JsonClient> {
  private final MessageLengthProtocol messageLengthProtocol;
  private final ObjectMapper mapper;

  public JsonProtocol(final MessageLengthProtocol messageLengthProtocol) {
    this.mapper = new ObjectMapper();
    this.messageLengthProtocol = messageLengthProtocol;
  }

  class JsonProtocolClient implements JsonClient {

    private final BytesClient bytesClient;

    public JsonProtocolClient(final BytesClient bytesClient) {
      this.bytesClient = bytesClient;
    }

    @Override
    public CompletionStage<Void> send(Object serializable) throws JsonProcessingException {
      return bytesClient.send(mapper.writeValueAsBytes(serializable));
    }

    @Override
    public <T> CompletionStage<T> receive(Class<T> clazz) {
      return bytesClient.receive().thenApply(bytes -> {
        try {
          return mapper.readValue(bytes, clazz);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  @Override
  public CompletionStage<JsonClient> accept() {
    return messageLengthProtocol.accept().thenApply(JsonProtocolClient::new);
  }

  @Override
  public CompletionStage<Void> startServer(int port) {
    return messageLengthProtocol.startServer(port);
  }

  @Override
  public int getListeningPort() {
    return messageLengthProtocol.getListeningPort();
  }

  @Override
  public CompletionStage<JsonClient> connect(InetSocketAddress inetSocketAddress) {
    return messageLengthProtocol.connect(inetSocketAddress).thenApply(JsonProtocolClient::new);
  }

  @Override
  public void close() throws IOException {
    messageLengthProtocol.close();
  }
}
