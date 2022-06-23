==================
DeltaSharing
==================

From the provider JSON we can easily instantiate our Java Connector using the DeltaSharing object, based on the provider JSON.
DeltaSharing instance is used to get access ot the table reader and to interact with the delta sharing server.

Examples
************

.. tabs::
   .. code-tab:: java

    import com.databricks.labs.delta.sharing.java.DeltaSharing;

    DeltaSharing sharing = DeltaSharing(
         providerJSON,
         "/dedicated/persisted/cache/location/"
      );


   .. code-tab:: scala

    import com.databricks.labs.delta.sharing.java.DeltaSharing

    val sharing = DeltaSharing(
         providerJSON,
         "/dedicated/persisted/cache/location/"
      )