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

public class TCPAsyncIO implements AsyncIO {
  private final Selector selector;
  private final int clientReceiveBufferSize;
  private Consumer<AsyncIOClient> clientAcceptedCallback;
  private BiConsumer<AsyncIOClient, ByteBuffer> clientDataReceivedCallback;
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
    private final ByteBuffer buffer;
    private int lastPosition = 0;
    private ConcurrentLinkedDeque<SendBuffer> sendBuffers = new ConcurrentLinkedDeque<>();
    private volatile SendBuffer currentlyWriting = null;

    /**
     * NOTE: TCPAsyncIOClient is currently not thread safe. onIsReadable() and onIsWritable() is expected
     * to be called from only one thread. send() however is thread-safe.
     *
     * @param channel
     */
    public TCPAsyncIOClient(final SocketChannel channel) {
      this.channel = channel;
      buffer = ByteBuffer.allocateDirect(clientReceiveBufferSize);
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
        lastPosition = 0;
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

    public CompletionStage<Void> send(final ByteBuffer byteBuffer) {
      final CompletableFuture<Void> sentFuture = new CompletableFuture<>();
      sendBuffers.addLast(new SendBuffer(sentFuture, byteBuffer));
      return sentFuture.thenCompose(ignored -> ensureStarted());
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
    final TCPAsyncIOClient client = new TCPAsyncIOClient(clientChannel);

    clientChannel.configureBlocking(false);
    clientChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, client);
    return client;
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
      serverChannel.configureBlocking(false);
      serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return ensureStarted();
  }

  @Override
  public CompletionStage<AsyncIOClient> connect(final InetSocketAddress address) {
    final CompletableFuture<Void> clientConnectedFuture = new CompletableFuture<>();

    final SocketChannel channel;
    try {
      channel = SocketChannel.open();
      channel.configureBlocking(true);
      channel.connect(address);
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

  @Override
  public int getListeningPort() {
    return listeningPort;
  }
}
