package org.deadlock.id2212.asyncio.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

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

    public JsonProtocolClient(final IntegerHeaderClient integerHeaderClient) {
      this.integerHeaderClient = integerHeaderClient;
    }

    @Override
    public CompletionStage<Void> send(Object serializable) {
      final Integer type = classToType.get(serializable.getClass());
      if (type == null) {
        throw new RuntimeException("Type for class " + serializable.getClass().getName() + " does not exists");
      }
      try {
        return integerHeaderClient.send(type, mapper.writeValueAsBytes(serializable));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public CompletionStage<IdJsonMessage> receive() {
      return integerHeaderClient.receive().thenApply(message ->
          new IdJsonMessage(mapper, typeToClass, message.getHeader(), message.getBytes()));
    }

    @Override
    public InetSocketAddress getAddress() {
      return integerHeaderClient.getAddress();
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
