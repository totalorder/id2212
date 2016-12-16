package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.AsyncIO;
import org.deadlock.id2212.asyncio.TCPAsyncIO;
import org.deadlock.id2212.asyncio.protocol.HeadedMessage;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.asyncio.protocol.IdJsonClient;
import org.deadlock.id2212.asyncio.protocol.IntegerHeaderProtocol;
import org.deadlock.id2212.asyncio.protocol.JsonProtocol;
import org.deadlock.id2212.asyncio.protocol.MessageLengthProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class JsonProtocolTest {

  private JsonProtocol jsonProtocol;

  @Before
  public void setUp() throws Exception {
    jsonProtocol = JsonProtocol.createDefault();
    jsonProtocol.registerType(TestJsonObject.class);
  }

  @After
  public void tearDown() throws Exception {
    jsonProtocol.close();
  }

  @Test
  public void canSendAndReceive() throws ExecutionException, InterruptedException, IOException {
    // Given
    // Connect + accept
    jsonProtocol.startServer(0).toCompletableFuture().get();
    IdJsonClient connectedClient = jsonProtocol.connect(new InetSocketAddress(jsonProtocol.getListeningPort()))
        .toCompletableFuture().get();

    CompletableFuture<IdJsonMessage> receivedMessageFuture = new CompletableFuture<>();
    IdJsonClient acceptedClient = jsonProtocol.accept().toCompletableFuture().get();
    acceptedClient.setOnMessageReceivedCallback(receivedMessageFuture::complete);

    TestJsonObject sentJsonMessage = new TestJsonObject(123, "Test text");

    // When
    connectedClient.send(sentJsonMessage).toCompletableFuture().get();

    // Then
    IdJsonMessage receivedJsonMessage = receivedMessageFuture.toCompletableFuture().get();
    assertEquals(sentJsonMessage, receivedJsonMessage.getObject(TestJsonObject.class));
  }

  @Test
  public void canReceiveReply() throws ExecutionException, InterruptedException, IOException {
    // Given
    // Connect + accept
    jsonProtocol.startServer(0).toCompletableFuture().get();
    IdJsonClient connectedClient = jsonProtocol.connect(new InetSocketAddress(jsonProtocol.getListeningPort()))
        .toCompletableFuture().get();

    IdJsonClient acceptedClient = jsonProtocol.accept().toCompletableFuture().get();

    TestJsonObject sentJsonMessage1 = new TestJsonObject(123, "Test text");
    TestJsonObject sentJsonMessage2 = new TestJsonObject(456, "Test text");

    UUID message1UUID = UUID.randomUUID();
    UUID message2UUID = UUID.randomUUID();

    // When
    CompletionStage<IdJsonMessage> receivedMessage2Fut = acceptedClient.receive(message2UUID);
    CompletionStage<IdJsonMessage> receivedMessage1Fut = acceptedClient.receive(message1UUID);

    connectedClient.send(sentJsonMessage1, message1UUID).toCompletableFuture().get();
    connectedClient.send(sentJsonMessage2, message2UUID).toCompletableFuture().get();



    // Then
    assertEquals(sentJsonMessage2, receivedMessage2Fut.toCompletableFuture().get().getObject(TestJsonObject.class));
    assertEquals(sentJsonMessage1, receivedMessage1Fut.toCompletableFuture().get().getObject(TestJsonObject.class));
  }
}