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

  private List<AsyncIO.Client> acceptedClients = new ArrayList<>();
  private Map<AsyncIO.Client, List<ByteBuffer>> dataReceivedBuffers = new HashMap<>();
  private AsyncIO asyncIO;
  private final CountDownLatch acceptLatch = new CountDownLatch(1);
  private CountDownLatch dataReceivedLatch = new CountDownLatch(1);

  @Before
  public void setUp() throws Exception {
    final Consumer<AsyncIO.Client> clientAcceptedCallback = client -> {
      acceptedClients.add(client);
      acceptLatch.countDown();
    };

    final BiConsumer<AsyncIO.Client, ByteBuffer> dataReceivedCallback = (client, byteBuffer) -> {
      if (!dataReceivedBuffers.containsKey(client)) {
        dataReceivedBuffers.put(client, new ArrayList<>());
      }
      dataReceivedBuffers.get(client).add(byteBuffer);
      dataReceivedLatch.countDown();
    };

    asyncIO = new AsyncIO(clientAcceptedCallback, dataReceivedCallback);
  }

  @After
  public void tearDown() throws Exception {
    asyncIO.close();
  }

  @Test
  public void canAccept() throws ExecutionException, InterruptedException, IOException {
    // Given
    asyncIO.startServer(0).toCompletableFuture().get();

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
    asyncIO.startServer(0).toCompletableFuture().get();
    final SocketChannel channel = SocketChannel.open();
    channel.configureBlocking(true);
    channel.connect(new InetSocketAddress(asyncIO.getListeningPort()));
    acceptLatch.await(1, TimeUnit.SECONDS);

    // When
    final int written = channel.write(ByteBuffer.wrap("Test data".getBytes("UTF-8")));

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
    asyncIO.startServer(0).toCompletableFuture().get();
    final SocketChannel channel = SocketChannel.open();
    channel.configureBlocking(true);
    channel.connect(new InetSocketAddress(asyncIO.getListeningPort()));
    acceptLatch.await(1, TimeUnit.SECONDS);
    // Write + wait for receive
    channel.write(ByteBuffer.wrap("Test data".getBytes("UTF-8")));
    dataReceivedLatch.await(1, TimeUnit.SECONDS);
    final ByteBuffer byteBuffer = dataReceivedBuffers.entrySet().iterator().next().getValue().get(0);
    assertEquals("Test data", stringFromUTF8Buffer(byteBuffer));

    dataReceivedLatch = new CountDownLatch(1);

    // When
    final int written = channel.write(ByteBuffer.wrap("Test data 2".getBytes("UTF-8")));

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
    asyncIO.startServer(0).toCompletableFuture().get();

    // When
    AsyncIO.Client client = asyncIO.connect(new InetSocketAddress(asyncIO.getListeningPort()))
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
    asyncIO.startServer(0).toCompletableFuture().get();
    AsyncIO.Client client = asyncIO.connect(new InetSocketAddress(asyncIO.getListeningPort()))
        .toCompletableFuture().get();
    acceptLatch.await(1, TimeUnit.SECONDS);

    // When
    final CompletionStage<Void> sentFuture = client.send("Test data".getBytes("UTF-8"));

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
    asyncIO.startServer(0).toCompletableFuture().get();
    AsyncIO.Client connectedClient = asyncIO.connect(new InetSocketAddress(asyncIO.getListeningPort()))
        .toCompletableFuture().get();
    acceptLatch.await(1, TimeUnit.SECONDS);
    AsyncIO.Client acceptedClient = acceptedClients.get(0);

    // Connected writes C --> A
    connectedClient.send("Test data".getBytes("UTF-8")).toCompletableFuture().get();

    // Accepted reads A <-- C
    dataReceivedLatch.await(1, TimeUnit.SECONDS);
    final ByteBuffer byteBuffer = dataReceivedBuffers.get(acceptedClient).get(0);
    assertEquals("Test data", stringFromUTF8Buffer(byteBuffer));

    // Accepted writes A --> C
    dataReceivedLatch = new CountDownLatch(1);
    acceptedClient.send("Test data 2".getBytes("UTF-8")).toCompletableFuture().get();

    // Connected reads C <-- A
    dataReceivedLatch.await(1, TimeUnit.SECONDS);
    assertEquals(2, dataReceivedBuffers.entrySet().size());
    final ByteBuffer byteBuffer2 = dataReceivedBuffers.get(connectedClient).get(0);
    assertEquals("Test data 2", stringFromUTF8Buffer(byteBuffer2));

    // Connected writes C --> A
    dataReceivedLatch = new CountDownLatch(1);
    connectedClient.send("Test data 3".getBytes("UTF-8")).toCompletableFuture().get();

    // Accepted reads A <-- C
    dataReceivedLatch.await(1, TimeUnit.SECONDS);
    final ByteBuffer byteBuffer3 = dataReceivedBuffers.get(acceptedClient).get(1);
    assertEquals("Test data 3", stringFromUTF8Buffer(byteBuffer3));
  }

  private String stringFromUTF8Buffer(ByteBuffer buffer) throws UnsupportedEncodingException {
    byte[] data = new byte[buffer.remaining()];
    buffer.get(data);

    return new String(data, "UTF-8");
  }
}