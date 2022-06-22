==================
Provider JSON
==================

The connector expects the `profile files <https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#profile-file-format>`__ to be provided as a JSON payload, which contains a user's credentials to access a Delta Sharing Server.
We advise that you store and retrieve this from a secure location, such as a key vault.

Examples
************

.. tabs::
   .. code-tab:: java

    String providerJSON = "{\n" +
        "  \"shareCredentialsVersion\": 1,\n" +
        "  \"endpoint\": \"https://sharing.delta.io/delta-sharing/\",\n" +
        "  \"bearerToken\": \"faaie590d541265bcab1f2de9813274bf233\"\n" +
        "  \"expirationTime\": \"2021-11-12T00:12:29.0Z\"\n" +
    "}";

   .. code-tab:: scala

    val providerJSON = """{
        "shareCredentialsVersion": 1,
        "endpoint": "https://sharing.endpoint/",
        "bearerToken": "faaieXXXXXXX…XXXXXXXX233",
        "expirationTime": "2021-11-12T00:12:29.0Z"
    }"""



