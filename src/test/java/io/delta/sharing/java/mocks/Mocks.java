package io.delta.sharing.java.mocks;

public class Mocks {
  public static String providerJson = "{\n" +
      "  \"shareCredentialsVersion\": 1,\n" +
      "  \"endpoint\": \"https://sharing.delta.io/delta-sharing/\",\n" +
      "  \"bearerToken\": \"faaie590d541265bcab1f2de9813274bf233\"\n" +
      "}";

  public static String ProviderJsonUnsupportedCredentials = "{\n" +
      "  \"shareCredentialsVersion\": 12,\n" +
      "  \"endpoint\": \"https://sharing.delta.io/delta-sharing/\",\n" +
      "  \"bearerToken\": \"faaie590d541265bcab1f2de9813274bf233\"\n" +
      "}";

  public static String ProviderJsonNoEndpoint = "{\n" +
      "  \"shareCredentialsVersion\": 12,\n" +
      "  \"bearerToken\": \"faaie590d541265bcab1f2de9813274bf233\"\n" +
      "}";

  public static String ProviderJsonNoToken = "{\n" +
      "  \"shareCredentialsVersion\": 1,\n" +
      "  \"endpoint\": \"https://sharing.delta.io/delta-sharing/\",\n" +
      "}";
}
