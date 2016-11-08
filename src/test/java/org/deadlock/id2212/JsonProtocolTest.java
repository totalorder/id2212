package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.AsyncIO;
import org.deadlock.id2212.asyncio.TCPAsyncIO;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.asyncio.protocol.IdJsonClient;
import org.deadlock.id2212.asyncio.protocol.IntegerHeaderProtocol;
import org.deadlock.id2212.asyncio.protocol.JsonProtocol;
import org.deadlock.id2212.asyncio.protocol.MessageLengthProtocol;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class JsonProtocolTest {

  private AsyncIO asyncIO;
  private MessageLengthProtocol messageLengthProtocol;
  private IntegerHeaderProtocol integerHeaderProtocol;
  private JsonProtocol jsonProtocol;

  @Before
  public void setUp() throws Exception {
    asyncIO = new TCPAsyncIO(5);
    messageLengthProtocol = new MessageLengthProtocol(asyncIO);
    integerHeaderProtocol = new IntegerHeaderProtocol(messageLengthProtocol);
    jsonProtocol = new JsonProtocol(integerHeaderProtocol);
    jsonProtocol.registerType(1, TestJsonObject.class);
  }

  @Test
  public void canSendAndReceive() throws ExecutionException, InterruptedException, IOException {
    // Given
    // Connect + accept
    jsonProtocol.startServer(0).toCompletableFuture().get();
    IdJsonClient connectedClient = jsonProtocol.connect(new InetSocketAddress(jsonProtocol.getListeningPort()))
        .toCompletableFuture().get();
    IdJsonClient acceptedClient = jsonProtocol.accept().toCompletableFuture().get();

    TestJsonObject sentJsonMessage = new TestJsonObject(123, "Test text");

    // When
    connectedClient.send(sentJsonMessage).toCompletableFuture().get();

    // Then
    IdJsonMessage receivedJsonMessage = acceptedClient.receive().toCompletableFuture().get();
    assertEquals(sentJsonMessage, receivedJsonMessage.getObject(TestJsonObject.class));
  }
}