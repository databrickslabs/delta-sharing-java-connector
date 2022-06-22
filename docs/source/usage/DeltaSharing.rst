==================
DeltaSharing
==================

From the provider JSON we can easily instantiate our Java Connector using the DeltaSharingFactory instance.
DeltaSharingFactory provides create API that returns and instance of DeltaSharing object based on the provider JSON.
DeltaSharing instance is used to get access ot the table reader and to interact with the delta sharing server.

Examples
************

.. tabs::
   .. code-tab:: java

    import io.delta.sharing.java.DeltaSharingFactory;
    import io.delta.sharing.java.DeltaSharing;

    DeltaSharing sharing = DeltaSharingFactory
      .create(
         providerJSON,
         "/dedicated/persisted/cache/location/"
      );


   .. code-tab:: scala

    import io.delta.sharing.java.DeltaSharingFactory

    val sharing = DeltaSharingFactory
      .create(
         providerJSON,
         "/dedicated/persisted/cache/location/"
      )