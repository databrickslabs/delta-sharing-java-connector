package io.delta.sharing.java.adaptor;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.delta.sharing.java.mocks.Mocks;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class DeltaSharingJSONProviderTestCase {

    @Test
    void testInstanceCreation() throws JsonProcessingException {
        String providerJSON = Mocks.providerJSON;
        DeltaSharingJSONProvider provider = new DeltaSharingJSONProvider(providerJSON);
        Assertions.assertAll(
                "asserting profile",
                () ->
                        Assertions.assertEquals(provider.getProfile().shareCredentialsVersion().get(), 1),
                () ->
                        Assertions.assertEquals(provider.getProfile().endpoint(), "https://sharing.delta.io/delta-sharing/"),
                () ->
                        Assertions.assertEquals(provider.getProfile().bearerToken(), "faaie590d541265bcab1f2de9813274bf233")
        );
    }

    @Test
    void testErrors() {
        Assertions.assertAll(
                "asserting profile",
                () ->
                        Assertions.assertThrows(IllegalArgumentException.class, () -> {
                            DeltaSharingJSONProvider providerUnsupportedCredentials = new DeltaSharingJSONProvider(Mocks.ProviderJSONUnsupportedCredentials);
                            providerUnsupportedCredentials.getProfile().shareCredentialsVersion().get();
                        }),
                () ->
                        Assertions.assertThrows(IllegalArgumentException.class, () -> {
                            DeltaSharingJSONProvider providerNoEndpoint = new DeltaSharingJSONProvider(Mocks.ProviderJSONNoEndpoint);
                            providerNoEndpoint.getProfile().shareCredentialsVersion().get();
                        }),
                () ->
                        Assertions.assertThrows(JsonParseException.class, () -> {
                            DeltaSharingJSONProvider providerNoToken = new DeltaSharingJSONProvider(Mocks.ProviderJSONNoToken);
                            providerNoToken.getProfile().shareCredentialsVersion().get();
                        })
        );
    }
}
