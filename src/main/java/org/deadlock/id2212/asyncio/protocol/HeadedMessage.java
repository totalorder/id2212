package org.deadlock.id2212.asyncio.protocol;

public class HeadedMessage<Header> {
  private final Header header;
  private final byte[] bytes;

  public HeadedMessage(final Header header, final byte[] bytes) {
    this.header = header;
    this.bytes = bytes;
  }

  public Header getHeader() {
    return header;
  }

  public byte[] getBytes() {
    return bytes;
  }
}
