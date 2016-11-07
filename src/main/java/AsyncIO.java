import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class AsyncIO implements Closeable {
  private final Selector selector;
  private final Consumer<Client> clientAcceptedCallback;
  private final BiConsumer<Client, ByteBuffer> clientDataReceivedCallback;
  private int listeningPort = 0;
  private volatile CompletableFuture<Void> started = null;
  private volatile CompletableFuture<Void> closed = null;

  @Override
  public void close() throws IOException {
    closed = new CompletableFuture<>();
    try {
      closed.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  class Client {
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
    private final ByteBuffer buffer;
    private int lastPosition = 0;
    private ConcurrentLinkedDeque<SendBuffer> sendBuffers = new ConcurrentLinkedDeque<>();
    private volatile SendBuffer currentlyWriting = null;

    /**
     * NOTE: Client is currently not thread safe. onIsReadable() and onIsWritable() is expected
     * to be called from only one thread. send() however is thread-safe.
     *
     * @param channel
     */
    public Client(final SocketChannel channel) {
      this.channel = channel;
      buffer = ByteBuffer.allocate(1024);
    }

    protected void onIsReadable() throws IOException {
      boolean receivedData = false;
      while (buffer.hasRemaining() && channel.read(buffer) > 0) {
        receivedData = true;
      }

      if (receivedData) {
        final ByteBuffer readOnlySlice = buffer.asReadOnlyBuffer();
        readOnlySlice.position(lastPosition);
        readOnlySlice.limit(buffer.position());
        lastPosition = buffer.position();

        clientDataReceivedCallback.accept(this, readOnlySlice);
      }

      if (!buffer.hasRemaining()) {
        buffer.clear();
      }
    }

    protected void onIsWritable() throws IOException {
      if (currentlyWriting == null && !sendBuffers.isEmpty()) {
        try {
          currentlyWriting = sendBuffers.removeFirst();
        } catch (NoSuchElementException e) {
          // It's empty, do nothing
        }
      }

      if (currentlyWriting != null) {
        while (currentlyWriting.byteBuffer.hasRemaining() &&
            channel.write(currentlyWriting.byteBuffer) > 0) {
        }

        if (!currentlyWriting.byteBuffer.hasRemaining()) {
          currentlyWriting.sentFuture.complete(null);
          currentlyWriting = null;
        }
      }
    }

    public CompletionStage<Void> send(final byte[] bytes) {
      return send(ByteBuffer.wrap(bytes));
    }

    public CompletionStage<Void> send(final ByteBuffer byteBuffer) {
      final CompletableFuture<Void> sentFuture = new CompletableFuture<>();
      sendBuffers.addLast(new SendBuffer(sentFuture, byteBuffer));
      return sentFuture;
    }
  }

  public AsyncIO(final Consumer<Client> clientAcceptedCallback,
                 final BiConsumer<Client, ByteBuffer> clientDataReceivedCallback) {
    this.clientAcceptedCallback = clientAcceptedCallback;
    this.clientDataReceivedCallback = clientDataReceivedCallback;
    try {
      selector = Selector.open();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void selectForever() {
    ForkJoinPool.commonPool().execute(() -> {
      try {
        if (selector.selectNow() > 0) {
          for (final SelectionKey key : selector.selectedKeys()) {
            if (key.isAcceptable()) {
              accept(key.channel());
            }

            if (key.isReadable()) {
              ((Client)key.attachment()).onIsReadable();
            }

            if (key.isWritable()) {
              ((Client)key.attachment()).onIsWritable();
            }
          }
          selector.selectedKeys().clear();
        }
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        if (!started.isDone()) {
          started.complete(null);
        }

        if (closed == null) {
          selectForever();
        } else if (!closed.isDone()) {
          closed.complete(null);
        }
      }
    });
  }

  private void accept(final SelectableChannel acceptableChannel) throws IOException {
    final SocketChannel clientChannel = ((ServerSocketChannel)acceptableChannel).accept();

    final Client client = createClient(clientChannel);

    clientAcceptedCallback.accept(client);
  }

  private Client createClient(final SocketChannel clientChannel) throws IOException {
    final Client client = new Client(clientChannel);

    clientChannel.configureBlocking(false);
    clientChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, client);
    return client;
  }


  public CompletionStage<Void> startServer(final int port) {
    if (started != null) {
      throw new RuntimeException("Server already started!");
    }
    started = new CompletableFuture<>();

    try {
      final ServerSocketChannel serverChannel = ServerSocketChannel.open();
      final ServerSocket serverSocket = serverChannel.socket();

      serverSocket.bind(new InetSocketAddress(port));
      listeningPort = serverSocket.getLocalPort();
      serverChannel.configureBlocking(false);
      serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    } catch (IOException e) {
      e.printStackTrace();
    }

    selectForever();

    return started;
  }

  public CompletionStage<Client> connect(final InetSocketAddress address) throws IOException {
    final SocketChannel channel = SocketChannel.open();
    channel.configureBlocking(true);
    channel.connect(address);
    final CompletableFuture<Void> clientConnectedFuture = new CompletableFuture<>();
    awaitConnection(channel, clientConnectedFuture);
    return clientConnectedFuture.thenApply(ignored -> {
      try {
        return createClient(channel);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

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

  public int getListeningPort() {
    return listeningPort;
  }
}
