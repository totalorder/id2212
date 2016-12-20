package org.deadlock.id2212.asyncio.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

public class IntegerHeaderProtocolClient
    extends HeaderProtocol.HeaderProtocolClient<IntegerUUIDHeader>
    implements IntegerHeaderClient {
  public IntegerHeaderProtocolClient(
      final BytesClient bytesClient) {
    super(bytesClient);
  }

  @Override
  byte[] serializeHeader(final IntegerUUIDHeader header) {
    return ByteBuffer.allocate(20)
        .putInt(header.integer)
        .putLong(header.uuid.getMostSignificantBits())
        .putLong(header.uuid.getLeastSignificantBits())
        .array();
  }

  @Override
  HeadedMessage<IntegerUUIDHeader> deserializeHeader(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final Integer integer = buffer.getInt();
    final UUID uuid = new UUID(buffer.getLong(), buffer.getLong());
    final byte[] message = new byte[buffer.remaining()];
    buffer.get(message);
    return new HeadedMessage<>(new IntegerUUIDHeader(integer, uuid), message);
  }
}
