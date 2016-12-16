package org.deadlock.id2212.asyncio.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class IdJsonMessage extends HeadedJson<IntegerUUIDHeader> {
  private final Map<Integer, Class> typeToClass;

  public IdJsonMessage(final ObjectMapper mapper, Map<Integer, Class> typeToClass, final IntegerUUIDHeader header, final byte[] bytes) {
    super(mapper, header, bytes);
    this.typeToClass = typeToClass;
  }

  public boolean isClass(final Class... classes) {
    for (Class clazz : classes) {
      if (clazz.equals(typeToClass.get(header.integer))) {
        return true;
      }
    }
    return false;
  }

  public <T> T getObject(final Class<T> clazz) {
    if (!clazz.equals(typeToClass.get(header.integer))) {
      throw new RuntimeException("Class for type" + header + " does not exists");
    }
    try {
      return super.getObject(clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public UUID getUUID() {
    return header.uuid;
  }

  public String getString() {
    return super.getString();
  }

  public String toString() {
    return getObject(typeToClass.get(header.integer)).getClass().getSimpleName() + getString();
  }
}
