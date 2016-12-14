package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.AsyncIO;
import org.deadlock.id2212.asyncio.TCPAsyncIO;
import org.deadlock.id2212.asyncio.protocol.HeadedMessage;
import org.deadlock.id2212.asyncio.protocol.IntegerUUIDHeader;
import org.deadlock.id2212.asyncio.protocol.MessageLengthProtocol;
import org.deadlock.id2212.asyncio.protocol.IntegerHeaderClient;
import org.deadlock.id2212.asyncio.protocol.IntegerHeaderProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class IntegerHeaderProtocolTest {

  private AsyncIO asyncIO;
  private MessageLengthProtocol messageLengthProtocol;
  private IntegerHeaderProtocol integerHeaderProtocol;

  @Before
  public void setUp() throws Exception {
    asyncIO = new TCPAsyncIO(5);
    messageLengthProtocol = new MessageLengthProtocol(asyncIO);
    integerHeaderProtocol = new IntegerHeaderProtocol(messageLengthProtocol);
  }

  @After
  public void tearDown() throws Exception {
    integerHeaderProtocol.close();
  }

  @Test
  public void canSendAndReceive() throws ExecutionException, InterruptedException, IOException {
    // Given
    // Connect + accept
    integerHeaderProtocol.startServer(0).toCompletableFuture().get();
    CompletableFuture<HeadedMessage<IntegerUUIDHeader>> receivedMessageFuture = new CompletableFuture<>();

    IntegerHeaderClient connectedClient = integerHeaderProtocol.connect(new InetSocketAddress(integerHeaderProtocol.getListeningPort()))
        .toCompletableFuture().get();


    IntegerHeaderClient acceptedClient = integerHeaderProtocol.accept().toCompletableFuture().get();
    acceptedClient.setOnMessageReceivedCallback(receivedMessageFuture::complete);

    // When
    final UUID uuid = UUID.randomUUID();
    connectedClient.send(new IntegerUUIDHeader(123, uuid), "Test message".getBytes("UTF-8")).toCompletableFuture().get();

    // Then


    HeadedMessage<IntegerUUIDHeader> receivedMessage = receivedMessageFuture.toCompletableFuture().get();
    assertEquals(123, receivedMessage.getHeader().integer);
    assertEquals(uuid, receivedMessage.getHeader().uuid);
    assertEquals("Test message", new String(receivedMessage.getBytes(), "UTF-8"));
  }
}