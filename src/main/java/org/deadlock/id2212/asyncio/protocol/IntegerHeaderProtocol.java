package org.deadlock.id2212.asyncio.protocol;

public class IntegerHeaderProtocol extends HeaderProtocol<IntegerHeaderClient> {
  public IntegerHeaderProtocol(final MessageLengthProtocol messageLengthProtocol) {
    super(messageLengthProtocol);
  }

  @Override
  IntegerHeaderClient createClient(final BytesClient bytesClient) {
    return new IntegerHeaderProtocolClient(bytesClient);
  }
}
