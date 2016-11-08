package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.AsyncIO;
import org.deadlock.id2212.asyncio.TCPAsyncIO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class MessageLengthProtocolTest {

  private AsyncIO asyncIO;
  private MessageLengthProtocol messageLengthProtocol;

  @Before
  public void setUp() throws Exception {
    asyncIO = new TCPAsyncIO();
    messageLengthProtocol = new MessageLengthProtocol(asyncIO);
  }

  @After
  public void tearDown() throws Exception {
    messageLengthProtocol.close();
  }

  @Test
  public void canAccept() throws ExecutionException, InterruptedException, IOException {
    // Given
    messageLengthProtocol.startServer(0).toCompletableFuture().get();

    // When
    final SocketChannel channel = SocketChannel.open();
    channel.configureBlocking(true);
    final boolean connected = channel.connect(new InetSocketAddress(messageLengthProtocol.getListeningPort()));

    // Then
    assertTrue(connected);
    final Client client = messageLengthProtocol.accept().toCompletableFuture().get();
    assertNotNull(client);
  }

  @Test
  public void canConnect() throws ExecutionException, InterruptedException, IOException {
    // Given
    messageLengthProtocol.startServer(0).toCompletableFuture().get();

    // When
    Client client = messageLengthProtocol.connect(new InetSocketAddress(messageLengthProtocol.getListeningPort()))
        .toCompletableFuture().get();

    // Then
    assertNotNull(client);
  }

  @Test
  public void canSendAndReceive() throws ExecutionException, InterruptedException, IOException {
    // Given
    // Connect + accept
    messageLengthProtocol.startServer(0).toCompletableFuture().get();
    Client connectedClient = messageLengthProtocol.connect(new InetSocketAddress(messageLengthProtocol.getListeningPort()))
        .toCompletableFuture().get();
    Client acceptedClient = messageLengthProtocol.accept().toCompletableFuture().get();

    // When
    connectedClient.send("Test message".getBytes("UTF-8")).toCompletableFuture().get();

    // Then
    final byte[] bytes = acceptedClient.receieve().toCompletableFuture().get();
    assertEquals("Test message", new String(bytes, "UTF-8"));
  }
}