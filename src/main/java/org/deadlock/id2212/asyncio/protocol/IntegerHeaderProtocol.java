package org.deadlock.id2212.asyncio.protocol;

/**
 * Header protocol that uses an Integer as header
 */
public class IntegerHeaderProtocol extends HeaderProtocol<IntegerHeaderClient> {
  public IntegerHeaderProtocol(final MessageLengthProtocol messageLengthProtocol) {
    super(messageLengthProtocol);
  }

  @Override
  IntegerHeaderClient createClient(final BytesClient bytesClient) {
    return new IntegerHeaderProtocolClient(bytesClient);
  }

  public static IntegerHeaderProtocol createDefault() {
    return new IntegerHeaderProtocol(MessageLengthProtocol.createDefault());
  }
}
