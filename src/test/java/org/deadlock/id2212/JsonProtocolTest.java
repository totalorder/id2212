package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.AsyncIO;
import org.deadlock.id2212.asyncio.TCPAsyncIO;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class JsonProtocolTest {

  private AsyncIO asyncIO;
  private MessageLengthProtocol messageLengthProtocol;
  private JsonProtocol jsonProtocol;

  @Before
  public void setUp() throws Exception {
    asyncIO = new TCPAsyncIO(5);
    messageLengthProtocol = new MessageLengthProtocol(asyncIO);
    jsonProtocol = new JsonProtocol(messageLengthProtocol);
  }

  @Test
  public void canSendAndReceive() throws ExecutionException, InterruptedException, IOException {
    // Given
    // Connect + accept
    jsonProtocol.startServer(0).toCompletableFuture().get();
    JsonClient connectedClient = jsonProtocol.connect(new InetSocketAddress(jsonProtocol.getListeningPort()))
        .toCompletableFuture().get();
    JsonClient acceptedClient = jsonProtocol.accept().toCompletableFuture().get();

    TestJsonMessage sentJsonMessage = new TestJsonMessage(123, "Test text");

    // When
    connectedClient.send(sentJsonMessage).toCompletableFuture().get();

    // Then
    TestJsonMessage receivedJsonMessage = acceptedClient.receive(TestJsonMessage.class).toCompletableFuture().get();
    assertEquals(sentJsonMessage, receivedJsonMessage);
  }
}