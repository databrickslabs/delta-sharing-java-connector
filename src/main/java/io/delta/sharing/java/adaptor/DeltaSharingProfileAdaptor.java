package io.delta.sharing.java.adaptor;

import io.delta.sharing.spark.DeltaSharingProfile;
import scala.Option;

//Suppressed since getters are used only by Jackson via reflection
@SuppressWarnings("unused")
public class DeltaSharingProfileAdaptor {
    int shareCredentialsVersion;
    String endpoint;
    String bearerToken;

    public DeltaSharingProfileAdaptor() {
    }

    public int getShareCredentialsVersion() {
        return shareCredentialsVersion;
    }

    public void setShareCredentialsVersion(int shareCredentialsVersion) {
        this.shareCredentialsVersion = shareCredentialsVersion;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getBearerToken() {
        return bearerToken;
    }

    public void setBearerToken(String bearerToken) {
        this.bearerToken = bearerToken;
    }

    public DeltaSharingProfile toProfile() {
        return DeltaSharingProfile.apply(Option.apply(shareCredentialsVersion), endpoint, bearerToken);
    }
}
