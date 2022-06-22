package io.delta.sharing.java.mocks;

public class Mocks {
    public static String providerJSON = "{\n" +
            "  \"shareCredentialsVersion\": 1,\n" +
            "  \"endpoint\": \"https://sharing.delta.io/delta-sharing/\",\n" +
            "  \"bearerToken\": \"faaie590d541265bcab1f2de9813274bf233\"\n" +
            "}";

    public static String ProviderJSONUnsupportedCredentials = "{\n" +
            "  \"shareCredentialsVersion\": 12,\n" +
            "  \"endpoint\": \"https://sharing.delta.io/delta-sharing/\",\n" +
            "  \"bearerToken\": \"faaie590d541265bcab1f2de9813274bf233\"\n" +
            "}";

    public static String ProviderJSONNoEndpoint = "{\n" +
            "  \"shareCredentialsVersion\": 12,\n" +
            "  \"bearerToken\": \"faaie590d541265bcab1f2de9813274bf233\"\n" +
            "}";

    public static String ProviderJSONNoToken = "{\n" +
            "  \"shareCredentialsVersion\": 1,\n" +
            "  \"endpoint\": \"https://sharing.delta.io/delta-sharing/\",\n" +
            "}";
}
