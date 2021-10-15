package io.delta.sharing.java.adaptor;

import io.delta.sharing.java.mocks.Mocks;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;


public class DeltaSharingJSONProviderTestCase {

    @Test
    void testInstanceCreation() {
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
}
