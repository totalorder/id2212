import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
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
    private final SocketChannel channel;
    private final ByteBuffer buffer;
    private int lastPosition = 0;

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
            } else if (key.isReadable()) {
              ((Client)key.attachment()).onIsReadable();
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
    final Client client = new Client(clientChannel);

    clientChannel.configureBlocking(false);
    clientChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, client);

    clientAcceptedCallback.accept(client);
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

  public int getListeningPort() {
    return listeningPort;
  }
}
