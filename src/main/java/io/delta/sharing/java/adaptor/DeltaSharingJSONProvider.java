package io.delta.sharing.java.adaptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.sharing.spark.DeltaSharingProfile;
import io.delta.sharing.spark.DeltaSharingProfileProvider;

/**
 * Load [[DeltaSharingProfile]] from a JSON string.
 */
public class DeltaSharingJSONProvider implements DeltaSharingProfileProvider {
    String configuration;
    DeltaSharingProfile profile;

    public DeltaSharingJSONProvider(String conf) {
        try {
            configuration = conf;
            ObjectMapper mapper = new ObjectMapper();
            DeltaSharingProfileAdaptor profileAdaptor = mapper.readValue(configuration, DeltaSharingProfileAdaptor.class);
            profile = profileAdaptor.toProfile();
        } catch (Exception e) {
            System.out.print(e);
        }
        if (profile.shareCredentialsVersion().isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot find the 'shareCredentialsVersion' field in the profile file");
        }

        if ((int) profile.shareCredentialsVersion().get() > DeltaSharingProfile.CURRENT()) {
            throw new IllegalArgumentException(
                    "'shareCredentialsVersion' in the profile is " +
                    "${profile.shareCredentialsVersion.get} which is too new. The current release " +
                    "supports version ${DeltaSharingProfile.CURRENT} and below. Please upgrade to a newer " +
                    "release.");
        }
        if (profile.endpoint() == null) {
            throw new IllegalArgumentException("Cannot find the 'endpoint' field in the profile file");
        }
        if (profile.bearerToken() == null) {
            throw new IllegalArgumentException("Cannot find the 'bearerToken' field in the profile file");
        }
    }


    @Override
    public DeltaSharingProfile getProfile() {
        return profile;
    }
}