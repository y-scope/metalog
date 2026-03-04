package com.yscope.metalog.common.config;

/** Configuration for a single S3-compatible object storage backend. */
public class ObjectStorageConfig {
  private String endpoint = "http://localhost:9000";
  private String accessKey = "minioadmin";
  private String secretKey = "minioadmin";
  private String region = "us-east-1";
  private boolean forcePathStyle = true;

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public boolean isForcePathStyle() {
    return forcePathStyle;
  }

  public void setForcePathStyle(boolean forcePathStyle) {
    this.forcePathStyle = forcePathStyle;
  }
}
