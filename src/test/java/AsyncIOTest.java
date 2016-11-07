import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class AsyncIOTest {

  private List<AsyncIO.Client> acceptedClients = new ArrayList<>();
  private List<AsyncIO.Client> dataReceivedClients = new ArrayList<>();
  private List<ByteBuffer> dataReceivedBuffers = new ArrayList<>();
  private AsyncIO asyncIO;
  private final CountDownLatch acceptLatch = new CountDownLatch(1);
  private CountDownLatch dataReceivedLatch = new CountDownLatch(1);

  @Before
  public void setUp() throws Exception {
    final Consumer<AsyncIO.Client> clientAcceptedCallback = client -> {
      acceptedClients.add(client);
      acceptLatch.countDown();
    };

    final BiConsumer<AsyncIO.Client, ByteBuffer> dataRecievedCallback = (client, byteBuffer) -> {
      dataReceivedClients.add(client);
      dataReceivedBuffers.add(byteBuffer);
      dataReceivedLatch.countDown();
    };

    asyncIO = new AsyncIO(clientAcceptedCallback, dataRecievedCallback);
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
    assertEquals(1, dataReceivedClients.size());
    final ByteBuffer byteBuffer = dataReceivedBuffers.get(0);
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
    final ByteBuffer byteBuffer = dataReceivedBuffers.get(0);
    assertEquals("Test data", stringFromUTF8Buffer(byteBuffer));

    dataReceivedLatch = new CountDownLatch(1);

    // When
    final int written = channel.write(ByteBuffer.wrap("Test data 2".getBytes("UTF-8")));

    // Then
    assertEquals(11, written);
    dataReceivedLatch.await(1, TimeUnit.SECONDS);
    assertEquals(2, dataReceivedClients.size());
    final ByteBuffer byteBuffer2 = dataReceivedBuffers.get(1);
    assertEquals("Test data 2", stringFromUTF8Buffer(byteBuffer2));
  }

  private String stringFromUTF8Buffer(ByteBuffer buffer) throws UnsupportedEncodingException {
    byte[] data = new byte[buffer.remaining()];
    buffer.get(data);

    return new String(data, "UTF-8");
  }
}