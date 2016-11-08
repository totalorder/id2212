package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class AsyncIOTest {

  private List<AsyncIOClient> acceptedClients = new ArrayList<>();
  private Map<AsyncIOClient, List<ByteBuffer>> dataReceivedBuffers = new HashMap<>();
  private AsyncIO asyncIO;
  private final CountDownLatch acceptLatch = new CountDownLatch(1);
  private CountDownLatch dataReceivedLatch = new CountDownLatch(1);
  private Consumer<AsyncIOClient> clientAcceptedCallback;
  private BiConsumer<AsyncIOClient, ByteBuffer> dataReceivedCallback;

  @Before
  public void setUp() throws Exception {
    clientAcceptedCallback = client -> {
      acceptedClients.add(client);
      acceptLatch.countDown();
    };

    dataReceivedCallback = (client, byteBuffer) -> {
      if (!dataReceivedBuffers.containsKey(client)) {
        dataReceivedBuffers.put(client, new ArrayList<>());
      }
      dataReceivedBuffers.get(client).add(byteBuffer);
      dataReceivedLatch.countDown();
    };

    asyncIO = new TCPAsyncIO();
  }

  @After
  public void tearDown() throws Exception {
    asyncIO.close();
  }

  @Test
  public void canAccept() throws ExecutionException, InterruptedException, IOException {
    // Given
    asyncIO.setClientDataReceivedCallback(dataReceivedCallback);
    asyncIO.startServer(0, clientAcceptedCallback).toCompletableFuture().get();

    // When
    final SocketChannel channel = SocketChannel.open();
    channel.configureBlocking(true);
    final boolean connected = channel.connect(new InetSocketAddress(asyncIO.getListeningPort()));

    // Then
    assertTrue(connected);
    acceptLatch.await(1, TimeUnit.SECONDS);
    assertEquals(1, acceptedClients.size());
  }

  @Test
  public void canReceive() throws ExecutionException, InterruptedException, IOException {
    // Given
    // Connect + wait for accept
    asyncIO.setClientDataReceivedCallback(dataReceivedCallback);
    asyncIO.startServer(0, clientAcceptedCallback).toCompletableFuture().get();
    final SocketChannel channel = SocketChannel.open();
    channel.configureBlocking(true);
    channel.connect(new InetSocketAddress(asyncIO.getListeningPort()));
    acceptLatch.await(1, TimeUnit.SECONDS);

    // When
    final int written = channel.write(utf8BufferFromString("Test data"));

    // Then
    assertEquals(9, written);
    dataReceivedLatch.await(1, TimeUnit.SECONDS);
    assertEquals(1, dataReceivedBuffers.size());
    final ByteBuffer byteBuffer = dataReceivedBuffers.entrySet().iterator().next().getValue().get(0);
    assertEquals("Test data", stringFromUTF8Buffer(byteBuffer));
  }

  @Test
  public void canReceiveMultipleMessages() throws ExecutionException, InterruptedException, IOException {
    // Given
    // Connect + wait for accept
    asyncIO.setClientDataReceivedCallback(dataReceivedCallback);
    asyncIO.startServer(0, clientAcceptedCallback).toCompletableFuture().get();
    final SocketChannel channel = SocketChannel.open();
    channel.configureBlocking(true);
    channel.connect(new InetSocketAddress(asyncIO.getListeningPort()));
    acceptLatch.await(1, TimeUnit.SECONDS);
    // Write + wait for receive
    channel.write(utf8BufferFromString("Test data"));
    dataReceivedLatch.await(1, TimeUnit.SECONDS);
    final ByteBuffer byteBuffer = dataReceivedBuffers.entrySet().iterator().next().getValue().get(0);
    assertEquals("Test data", stringFromUTF8Buffer(byteBuffer));

    dataReceivedLatch = new CountDownLatch(1);

    // When
    final int written = channel.write(utf8BufferFromString("Test data 2"));

    // Then
    assertEquals(11, written);
    dataReceivedLatch.await(1, TimeUnit.SECONDS);
    assertEquals(2, dataReceivedBuffers.entrySet().iterator().next().getValue().size());
    final ByteBuffer byteBuffer2 = dataReceivedBuffers.entrySet().iterator().next().getValue().get(1);
    assertEquals("Test data 2", stringFromUTF8Buffer(byteBuffer2));
  }

  @Test
  public void canConnect() throws ExecutionException, InterruptedException, IOException {
    // Given
    asyncIO.setClientDataReceivedCallback(dataReceivedCallback);
    asyncIO.startServer(0, clientAcceptedCallback).toCompletableFuture().get();

    // When
    AsyncIOClient client = asyncIO.connect(new InetSocketAddress(asyncIO.getListeningPort()))
        .toCompletableFuture().get();

    // Then
    assertNotNull(client);
    acceptLatch.await(1, TimeUnit.SECONDS);
    assertEquals(1, acceptedClients.size());
  }

  @Test
  public void canSend() throws ExecutionException, InterruptedException, IOException {
    // Given
    // Connect + wait for accept
    asyncIO.setClientDataReceivedCallback(dataReceivedCallback);
    asyncIO.startServer(0, clientAcceptedCallback).toCompletableFuture().get();
    AsyncIOClient client = asyncIO.connect(new InetSocketAddress(asyncIO.getListeningPort()))
        .toCompletableFuture().get();
    acceptLatch.await(1, TimeUnit.SECONDS);

    // When
    final CompletionStage<Void> sentFuture = client.send(utf8BufferFromString("Test data"));

    // Then
    dataReceivedLatch.await(1, TimeUnit.SECONDS);
    sentFuture.toCompletableFuture().get();
    assertEquals(1, dataReceivedBuffers.entrySet().iterator().next().getValue().size());
    final ByteBuffer byteBuffer2 = dataReceivedBuffers.entrySet().iterator().next().getValue().get(0);
    assertEquals("Test data", stringFromUTF8Buffer(byteBuffer2));
  }

  @Test
  public void canSendAndReceiveInterleavedMessages() throws ExecutionException, InterruptedException, IOException {
    // Given
    // Connect + wait for accept
    asyncIO.setClientDataReceivedCallback(dataReceivedCallback);
    asyncIO.startServer(0, clientAcceptedCallback).toCompletableFuture().get();
    AsyncIOClient connectedClient = asyncIO.connect(new InetSocketAddress(asyncIO.getListeningPort()))
        .toCompletableFuture().get();
    acceptLatch.await(1, TimeUnit.SECONDS);
    AsyncIOClient acceptedClient = acceptedClients.get(0);

    // Connected writes C --> A
    connectedClient.send(utf8BufferFromString("Test data")).toCompletableFuture().get();

    // Accepted reads A <-- C
    dataReceivedLatch.await(1, TimeUnit.SECONDS);
    final ByteBuffer byteBuffer = dataReceivedBuffers.get(acceptedClient).get(0);
    assertEquals("Test data", stringFromUTF8Buffer(byteBuffer));

    // Accepted writes A --> C
    dataReceivedLatch = new CountDownLatch(1);
    acceptedClient.send(utf8BufferFromString("Test data 2")).toCompletableFuture().get();

    // Connected reads C <-- A
    dataReceivedLatch.await(1, TimeUnit.SECONDS);
    assertEquals(2, dataReceivedBuffers.entrySet().size());
    final ByteBuffer byteBuffer2 = dataReceivedBuffers.get(connectedClient).get(0);
    assertEquals("Test data 2", stringFromUTF8Buffer(byteBuffer2));

    // Connected writes C --> A
    dataReceivedLatch = new CountDownLatch(1);
    connectedClient.send(utf8BufferFromString("Test data 3")).toCompletableFuture().get();

    // Accepted reads A <-- C
    dataReceivedLatch.await(1, TimeUnit.SECONDS);
    final ByteBuffer byteBuffer3 = dataReceivedBuffers.get(acceptedClient).get(1);
    assertEquals("Test data 3", stringFromUTF8Buffer(byteBuffer3));
  }

  private String stringFromUTF8Buffer(final ByteBuffer buffer) throws UnsupportedEncodingException {
    byte[] data = new byte[buffer.remaining()];
    buffer.get(data);

    return new String(data, "UTF-8");
  }

  private ByteBuffer utf8BufferFromString(final String string) throws UnsupportedEncodingException {
    return ByteBuffer.wrap(string.getBytes("UTF-8"));
  }
}