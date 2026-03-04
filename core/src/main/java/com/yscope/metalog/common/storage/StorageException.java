package com.yscope.metalog.common.storage;

/** Exception for storage operations across all backend types. */
public class StorageException extends Exception {
  public StorageException(String message) {
    super(message);
  }

  public StorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
