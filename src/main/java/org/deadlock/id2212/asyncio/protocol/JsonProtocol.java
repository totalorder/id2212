package org.deadlock.id2212.asyncio.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class JsonProtocol implements Protocol<IdJsonClient> {
  private final IntegerHeaderProtocol integerHeaderProtocol;
  private final ObjectMapper mapper;
  private final Map<Integer, Class> typeToClass = new HashMap<>();
  private final Map<Class, Integer> classToType = new HashMap<>();

  public JsonProtocol(final IntegerHeaderProtocol integerHeaderProtocol) {
    this.mapper = new ObjectMapper();
    this.integerHeaderProtocol = integerHeaderProtocol;
  }

  public void registerType(final int type, final Class clazz) {
    typeToClass.put(type, clazz);
    classToType.put(clazz, type);
  }

  class JsonProtocolClient implements IdJsonClient {

    private final IntegerHeaderClient integerHeaderClient;

    public JsonProtocolClient(final IntegerHeaderClient integerHeaderClient) {
      this.integerHeaderClient = integerHeaderClient;
    }

    @Override
    public CompletionStage<Void> send(Object serializable) throws JsonProcessingException {
      final Integer type = classToType.get(serializable.getClass());
      if (type == null) {
        throw new RuntimeException("Type for class " + serializable.getClass().getName() + " does not exists");
      }
      return integerHeaderClient.send(type, mapper.writeValueAsBytes(serializable));
    }

    @Override
    public CompletionStage<IdJsonMessage> receive() {
      return integerHeaderClient.receive().thenApply(message ->
          new IdJsonMessage(mapper, typeToClass, message.getHeader(), message.getBytes()));
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
  public CompletionStage<IdJsonClient> connect(InetSocketAddress inetSocketAddress) {
    return integerHeaderProtocol.connect(inetSocketAddress).thenApply(JsonProtocolClient::new);
  }

  @Override
  public void close() throws IOException {
    integerHeaderProtocol.close();
  }
}
