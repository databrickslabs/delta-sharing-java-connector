package com.databricks.labs.delta.sharing.java.adaptor;

import com.databricks.labs.delta.sharing.java.mocks.Mocks;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Objects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


/** 
 * Test cases for Delta Sharing Json Provider.
 */

public class DeltaSharingJsonProviderTestCase {

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  void testInstanceCreation() throws JsonProcessingException {
    String providerJson = Mocks.providerJson;
    DeltaSharingJsonProvider provider = new DeltaSharingJsonProvider(providerJson);
    Assertions.assertAll("asserting profile",
        () -> Assertions.assertEquals(provider.getProfile().shareCredentialsVersion().get(), 1),
        () -> Assertions.assertEquals(provider.getProfile().endpoint(),
            "https://sharing.delta.io/delta-sharing/"),
        () -> Assertions.assertEquals(provider.getProfile().bearerToken(),
            "faaie590d541265bcab1f2de9813274bf233"));

    Assertions.assertDoesNotThrow(() -> {
      DeltaSharingProfileAdaptor adaptor = new DeltaSharingProfileAdaptor();
      adaptor
          .setShareCredentialsVersion((int) provider.getProfile().shareCredentialsVersion().get());
      adaptor.setEndpoint(provider.getProfile().endpoint());
      adaptor.setBearerToken(provider.getProfile().bearerToken());
      adaptor.setExpirationTime(provider.getProfile().expirationTime());
      Objects.equals(adaptor.getShareCredentialsVersion(),
          provider.getProfile().shareCredentialsVersion().get());
      Objects.equals(adaptor.getEndpoint(), provider.getProfile().endpoint());
      Objects.equals(adaptor.getBearerToken(), provider.getProfile().bearerToken());
      Objects.equals(adaptor.getExpirationTime(), provider.getProfile().expirationTime());
    }, "adaptors format should match the provider format.");
  }

  @Test
  void testErrors() {
    Assertions.assertAll("asserting profile",
        () -> Assertions.assertThrows(IllegalArgumentException.class, () -> {
          DeltaSharingJsonProvider providerUnsupportedCredentials =
              new DeltaSharingJsonProvider(Mocks.ProviderJsonUnsupportedCredentials);
          providerUnsupportedCredentials.getProfile().shareCredentialsVersion().get();
        }), () -> Assertions.assertThrows(IllegalArgumentException.class, () -> {
          DeltaSharingJsonProvider providerNoEndpoint =
              new DeltaSharingJsonProvider(Mocks.ProviderJsonNoEndpoint);
          providerNoEndpoint.getProfile().shareCredentialsVersion().get();
        }), () -> Assertions.assertThrows(JsonParseException.class, () -> {
          DeltaSharingJsonProvider providerNoToken =
              new DeltaSharingJsonProvider(Mocks.ProviderJsonNoToken);
          providerNoToken.getProfile().shareCredentialsVersion().get();
        }));
  }
}
