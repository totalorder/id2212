package org.deadlock.id2212.asyncio.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class HeadedJson<Header> {
  private final ObjectMapper mapper;
  protected final Header header;
  private final byte[] bytes;

  public HeadedJson(final ObjectMapper mapper, final Header header, final byte[] bytes) {
    this.mapper = mapper;
    this.header = header;
    this.bytes = bytes;
  }

  public Header getHeader() {
    return header;
  }

  public <T> T getObject(final Class<T> clazz) throws IOException {
    return mapper.readValue(bytes, clazz);
  }
}
