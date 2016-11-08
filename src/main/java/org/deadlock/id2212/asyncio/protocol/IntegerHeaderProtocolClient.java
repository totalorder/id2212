package org.deadlock.id2212.asyncio.protocol;

import java.nio.ByteBuffer;

public class IntegerHeaderProtocolClient
    extends HeaderProtocol.HeaderProtocolClient<Integer>
    implements IntegerHeaderClient {
  public IntegerHeaderProtocolClient(
      final BytesClient bytesClient) {
    super(bytesClient);
  }

  @Override
  byte[] serializeHeader(final Integer header) {
    return ByteBuffer.allocate(4).putInt(header).array();
  }

  @Override
  HeadedMessage<Integer> deserializeHeader(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final Integer header = buffer.getInt();
    final byte[] message = new byte[buffer.remaining()];
    buffer.get(message);
    return new HeadedMessage<>(header, message);
  }
}
