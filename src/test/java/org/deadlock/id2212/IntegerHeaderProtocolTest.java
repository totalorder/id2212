package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.AsyncIO;
import org.deadlock.id2212.asyncio.TCPAsyncIO;
import org.deadlock.id2212.asyncio.protocol.HeadedMessage;
import org.deadlock.id2212.asyncio.protocol.MessageLengthProtocol;
import org.deadlock.id2212.asyncio.protocol.IntegerHeaderClient;
import org.deadlock.id2212.asyncio.protocol.IntegerHeaderProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
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
    IntegerHeaderClient connectedClient = integerHeaderProtocol.connect(new InetSocketAddress(integerHeaderProtocol.getListeningPort()))
        .toCompletableFuture().get();


    IntegerHeaderClient acceptedClient = integerHeaderProtocol.accept().toCompletableFuture().get();

    // When
    connectedClient.send(123, "Test message".getBytes("UTF-8")).toCompletableFuture().get();

    // Then
    HeadedMessage<Integer> receivedMessage = acceptedClient.receive().toCompletableFuture().get();
    assertEquals(123, receivedMessage.getHeader().intValue());
    assertEquals("Test message", new String(receivedMessage.getBytes(), "UTF-8"));
  }
}