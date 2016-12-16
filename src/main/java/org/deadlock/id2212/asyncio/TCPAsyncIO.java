package org.deadlock.id2212.asyncio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Listens to connections and receives data using a java.nio.channels.Selector,
 * providing accepted connections and received data using callbacks.
 * Enable connecting to remote servers
 * Enable sending data
 *
 * Data received has to be read immediately to be sure that the buffer has not been overwritten
 */
public class TCPAsyncIO implements AsyncIO {
  private final Selector selector;
  private final int clientReceiveBufferSize;
  private Consumer<AsyncIOClient> clientAcceptedCallback;
  private BiConsumer<AsyncIOClient, ByteBuffer> clientDataReceivedCallback;
  private int listeningPort = 0;
  private volatile CompletableFuture<Void> started = null;
  private volatile CompletableFuture<Void> closed = null;
  private InetSocketAddress listeningAddress;

  @Override
  public void close() throws IOException {
    if (started == null) {
      return;
    }

    if (closed == null) {
      closed = new CompletableFuture<>();
    }
    try {
      closed.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  class TCPAsyncIOClient implements AsyncIOClient {
    class SendBuffer {
      private final CompletableFuture<Void> sentFuture;
      private final ByteBuffer byteBuffer;

      private SendBuffer(final CompletableFuture<Void> sentFuture,
                         final ByteBuffer byteBuffer) {
        this.sentFuture = sentFuture;
        this.byteBuffer = byteBuffer;
      }
    }

    private final SocketChannel channel;
    private final Selector selector;
    private final ByteBuffer buffer;
    private int lastPosition = 0;
    private ConcurrentLinkedDeque<SendBuffer> sendBuffers = new ConcurrentLinkedDeque<>();
    private volatile SendBuffer currentlyWriting = null;

    /**
     * Calls data received callback when data is received
     * Enables sending data
     *
     * Data received has to be read immediately to be sure that the buffer has not been overwritten
     *
     * NOTE: TCPAsyncIOClient is not thread safe. onIsReadable() and onIsWritable() is expected
     * to be called from only one thread. send() however is thread-safe.
     *
     * @param channel
     * @param selector
     */
    public TCPAsyncIOClient(final SocketChannel channel, final Selector selector) {
      this.channel = channel;
      this.selector = selector;
      buffer = ByteBuffer.allocateDirect(clientReceiveBufferSize);
    }

    public void readyToReadAndWrite() {
      try {
        channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, this);
      } catch (ClosedChannelException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Read data from channel into a buffer. Call clientDataReceivedCallback with a
     * read-only slice of the buffer, representing the data read.
     */
    protected void onIsReadable() throws IOException {
      boolean receivedData = false;
      while (buffer.hasRemaining() && channel.read(buffer) > 0) {
        receivedData = true;
      }

      if (receivedData) {
        // Create a read-only slice with a view of the data to be read
        final ByteBuffer readOnlySlice = buffer.asReadOnlyBuffer();
        readOnlySlice.position(lastPosition);
        readOnlySlice.limit(buffer.position());
        lastPosition = buffer.position();

        clientDataReceivedCallback.accept(this, readOnlySlice);
      }

      if (!buffer.hasRemaining()) {
        buffer.clear();
        lastPosition = 0;
      }
    }

    /**
     * Write data from active buffer to channel. If buffer is written, try to
     * pop a new buffer from the queue and make it active.
     * Notify client that all data has been written by completing the corresponding future.
     */
    protected void onIsWritable() throws IOException {
      // Pop a new message for writing if there is not an active one and a new is available
      if (currentlyWriting == null && !sendBuffers.isEmpty()) {
        try {
          currentlyWriting = sendBuffers.removeFirst();
        } catch (NoSuchElementException e) {
          // It's empty, do nothing
        }
      }

      if (currentlyWriting != null) {
        // Write as many bytes as possible from active buffer
        while (currentlyWriting.byteBuffer.hasRemaining() &&
            channel.write(currentlyWriting.byteBuffer) > 0) {
        }

        // Discard buffer and notify listeners if it's fully written
        if (!currentlyWriting.byteBuffer.hasRemaining()) {
          currentlyWriting.sentFuture.complete(null);
          currentlyWriting = null;
        }
      }
    }

    public CompletionStage<Void> send(final ByteBuffer byteBuffer) {
      final CompletableFuture<Void> sentFuture = new CompletableFuture<>();
      sendBuffers.addLast(new SendBuffer(sentFuture, byteBuffer));
      return sentFuture.thenCompose(ignored -> ensureStarted());
    }

    @Override
    public InetSocketAddress getAddress() {
      return (InetSocketAddress)channel.socket().getRemoteSocketAddress();
    }
  }

  public TCPAsyncIO() {
    this(1024);
  }

  public TCPAsyncIO(final int clientReceiveBufferSize) {
    this.clientReceiveBufferSize = clientReceiveBufferSize;
    try {
      selector = Selector.open();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Poll the selector for readable/writable/acceptable channels
   * and call the corresponding Clients or create new Client on accept
   */
  private void selectForever() {
    ForkJoinPool.commonPool().execute(() -> {
      try {
        if (selector.selectNow() > 0) {
          for (final SelectionKey key : selector.selectedKeys()) {
            if (key.isAcceptable()) {
              accept(key.channel());
            }

            if (key.isReadable()) {
              ((TCPAsyncIOClient)key.attachment()).onIsReadable();
            }

            if (key.isWritable()) {
              ((TCPAsyncIOClient)key.attachment()).onIsWritable();
            }
          }
          selector.selectedKeys().clear();
        }
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        if (started != null && !started.isDone()) {
          started.complete(null);
        }

        if (closed == null) {
          selectForever();
        } else if (!closed.isDone()) {
          doClose();
        }
      }
    });
  }

  private void doClose() {
    try {
      selector.keys().stream().map(key -> {
        try {
          key.channel().close();
        } catch (IOException e) {
          e.printStackTrace();
        }
        return null;
      });
      selector.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    closed.complete(null);
  }

  private void accept(final SelectableChannel acceptableChannel) throws IOException {
    final SocketChannel clientChannel = ((ServerSocketChannel)acceptableChannel).accept();

    final TCPAsyncIOClient client = createClient(clientChannel);

    clientAcceptedCallback.accept(client);
  }

  private TCPAsyncIOClient createClient(final SocketChannel clientChannel) throws IOException {
    clientChannel.configureBlocking(false);
    return new TCPAsyncIOClient(clientChannel, selector);
  }

  private synchronized CompletionStage<Void> ensureStarted() {
    if (started != null) {
      return started;
    }
    if (clientDataReceivedCallback == null) {
      throw new RuntimeException("setClientDataReceivedCallback() has to be called before using AsyncIO");
    }

    started = new CompletableFuture<>();
    selectForever();
    return started;
  }

  @Override
  public void setClientDataReceivedCallback(final BiConsumer<AsyncIOClient, ByteBuffer> clientDataReceivedCallback) {
    this.clientDataReceivedCallback = clientDataReceivedCallback;
  }

  @Override
  public CompletionStage<Void> startServer(final int port,
                                           final Consumer<AsyncIOClient> clientAcceptedCallback) {

    this.clientAcceptedCallback = clientAcceptedCallback;

    try {
      final ServerSocketChannel serverChannel = ServerSocketChannel.open();
      final ServerSocket serverSocket = serverChannel.socket();

      serverSocket.bind(new InetSocketAddress(port));
      listeningPort = serverSocket.getLocalPort();
      listeningAddress = (InetSocketAddress)serverSocket.getLocalSocketAddress();
      serverChannel.configureBlocking(false);
      serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return ensureStarted();
  }

  @Override
  public CompletionStage<AsyncIOClient> connect(final InetSocketAddress address) {
    return ensureStarted().thenCompose(ignore -> {
      final CompletableFuture<Void> clientConnectedFuture = new CompletableFuture<>();

      final SocketChannel channel;
      try {
        channel = SocketChannel.open();
        channel.configureBlocking(true);
        channel.connect(address);

        // Start polling the connected-state to notify listeners when connected
        awaitConnection(channel, clientConnectedFuture);
      } catch (IOException e) {
        clientConnectedFuture.completeExceptionally(new RuntimeException(e));
        return clientConnectedFuture.thenApply(ignored -> null);
      }

      return clientConnectedFuture.thenApply(ignored -> {
        try {
          return createClient(channel);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    });
  }

  /**
   * Poll the channel for the connected-state before completing the associated future
   */
  private void awaitConnection(final SocketChannel channel,
                               final CompletableFuture<Void> connectedFuture) {
    ForkJoinPool.commonPool().execute(() -> {
      try {
        if (channel.finishConnect()) {
          connectedFuture.complete(null);
        } else {
          awaitConnection(channel, connectedFuture);
        }
      } catch (IOException e) {
        connectedFuture.completeExceptionally(e);
      }
    });
  }

  @Override
  public int getListeningPort() {
    return listeningPort;
  }

  @Override
  public InetSocketAddress getListeningAddress() {
    return listeningAddress;
  }
}
