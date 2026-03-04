package com.yscope.metalog.common.storage;

/** Exception for object not found (already deleted or doesn't exist). */
public class ObjectNotFoundException extends Exception {
  public ObjectNotFoundException(String message) {
    super(message);
  }
}
