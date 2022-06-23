package com.databricks.labs.delta.sharing.java.adaptor;

import io.delta.sharing.spark.DeltaSharingProfile;
import scala.Option;

/**
 * Used for Jackson JSON parsing and object mapping. POJO corresponding to profile file format.
 * Update when the format of profile file changes.
 *
 * @implNote Suppress is added because of all the getters and setters are required to be explicitly
 * created for Jackson to parse JSONs correctly. However warnings are shown since getters
 * and setters are not explicitly tests.
 * @since 0.1.0
 */
public class DeltaSharingProfileAdaptor {
  private int shareCredentialsVersion;
  private String endpoint;
  private String bearerToken;
  private String expirationTime;

  /**
   * Default constructor.
   */
  public DeltaSharingProfileAdaptor() {
  }

  /**
   * Getter for shareCredentialsVersion.
   */
  public int getShareCredentialsVersion() {
    return shareCredentialsVersion;
  }

  /**
   * Setter for shareCredentialsVersion.
   */
  public void setShareCredentialsVersion(int shareCredentialsVersion) {
    this.shareCredentialsVersion = shareCredentialsVersion;
  }

  /**
   * Getter for endpoint.
   */
  public String getEndpoint() {
    return endpoint;
  }

  /**
   * Setter for endpoint.
   */
  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  /**
   * Getter for bearerToken.
   */
  public String getBearerToken() {
    return bearerToken;
  }

  /**
   * Setter for bearerToken.
   */
  public void setBearerToken(String bearerToken) {
    this.bearerToken = bearerToken;
  }

  /**
   * Getter for expirationTime.
   */
  public String getExpirationTime() {
    return expirationTime;
  }

  /**
   * Setter for expirationTime.
   */
  public void setExpirationTime(String expirationTime) {
    this.expirationTime = expirationTime;
  }

  /**
   * Java wrapper method that can generate Scala paired object. This is need to be able to abstract
   * from cross language APIs.
   *
   * @return An equivalent instance of Scala {@link DeltaSharingProfile}
   * class.
   */
  public DeltaSharingProfile toProfile() {
    return DeltaSharingProfile.apply(Option.apply(shareCredentialsVersion), endpoint, bearerToken,
        expirationTime);
  }
}
