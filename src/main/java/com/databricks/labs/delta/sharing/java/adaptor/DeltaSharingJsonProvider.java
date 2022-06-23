package com.databricks.labs.delta.sharing.java.adaptor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.sharing.spark.DeltaSharingProfile;
import io.delta.sharing.spark.DeltaSharingProfileProvider;


/**
 * Loads {@link DeltaSharingProfileProvider} based on configuration provided as
 * a JSON document. JSON document has to be a valid profile file document.
 * Required fields are checked inside the constructor method and the object
 * creation will fail in case of breaking the constraints.
 * <p/>
 *
 * @see DeltaSharingProfileAdaptor DeltaSharingProfileAdaptor used for object
 * mapping when parsing JSON.
 * @since 0.1.0
 */
public class DeltaSharingJsonProvider implements DeltaSharingProfileProvider {
  private final DeltaSharingProfile profile;

  /**
   * Constructor method that expects Configuration JSON. JSON file must contain
   * endpoint, bearerToken and shareCredentialsVersion fields. Object creation
   * will fail if shareCredentialsVersion is too new.
   *
   * @param conf A valid JSON object.
   */
  public DeltaSharingJsonProvider(String conf) throws JsonProcessingException {
    try {
      ObjectMapper mapper = new ObjectMapper();
      DeltaSharingProfileAdaptor profileAdaptor =
          mapper.readValue(conf, DeltaSharingProfileAdaptor.class);
      profile = profileAdaptor.toProfile();
    } catch (Exception e) {
      System.out.print(e);
      throw e;
    }

    if ((int) profile.shareCredentialsVersion().get() > DeltaSharingProfile
        .CURRENT()) {
      throw new IllegalArgumentException(
          "'shareCredentialsVersion' in the profile is "
              + "${profile.shareCredentialsVersion.get} which is too new."
              + "The current release supports version ${DeltaSharingProfile.CURRENT}"
              + " and below. Please upgrade to a newerrelease.");
    }
    if (profile.endpoint() == null) {
      throw new IllegalArgumentException(
          "Cannot find the 'endpoint' field in the profile file");
    }
  }


  @Override
  public DeltaSharingProfile getProfile() {
    return profile;
  }
}
