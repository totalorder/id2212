package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.AsyncIO;
import org.deadlock.id2212.asyncio.AsyncIOClient;
import org.deadlock.id2212.asyncio.protocol.MessageLengthProtocol;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class MessageLengthProtocolClientTest {

  private MessageLengthProtocol.MessageLengthProtocolClient client;

  @Before
  public void setUp() throws Exception {
    AsyncIOClient loopbackClient = new AsyncIOClient() {
      @Override
      public void close() throws IOException {

      }

      public CompletionStage<Void> send(final ByteBuffer byteBuffer) {
        client.onDataReceived(byteBuffer);
        return new CompletableFuture<>();
      }

      @Override
      public InetSocketAddress getAddress() {
        return null;
      }

      @Override
      public void readyToReadAndWrite() {
      }
    };

    client = new MessageLengthProtocol.MessageLengthProtocolClient(loopbackClient);
  }



  @Test
  public void canSendAndReceive() throws UnsupportedEncodingException, ExecutionException, InterruptedException {
    final CompletionStage<byte[]> receive = client.receive();
    client.send("Test message".getBytes("UTF-8"));
    final String result = receive.thenApply(this::stringFromUTF8Bytes).toCompletableFuture().get();
    assertEquals("Test message", result);
  }

  private String stringFromUTF8Bytes(final byte[] bytes) {
    try {
      return new String(bytes, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}