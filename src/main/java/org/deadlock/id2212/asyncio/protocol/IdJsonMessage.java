package org.deadlock.id2212.asyncio.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class IdJsonMessage extends HeadedJson<Integer> {
  private final Map<Integer, Class> typeToClass;

  public IdJsonMessage(final ObjectMapper mapper, Map<Integer, Class> typeToClass, final Integer header, final byte[] bytes) {
    super(mapper, header, bytes);
    this.typeToClass = typeToClass;
  }

  public boolean isClass(final Class clazz) {
    return clazz.equals(typeToClass.get(header));
  }

  public <T> T getObject(final Class<T> clazz) {
    if (!clazz.equals(typeToClass.get(header))) {
      throw new RuntimeException("Class for type" + header + " does not exists");
    }
    try {
      return super.getObject(clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
