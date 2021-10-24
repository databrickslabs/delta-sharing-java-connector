package io.delta.sharing.java;

import io.delta.sharing.java.adaptor.DeltaSharingJSONProvider;
import io.delta.sharing.spark.DeltaSharingProfileProvider;
import io.delta.sharing.spark.DeltaSharingRestClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

@SuppressWarnings("unused")
public class DeltaSharingFactory {

    public static DeltaSharing create(DeltaSharingProfileProvider profileProvider, Path checkpointPath) throws IOException {
        DeltaSharing instance = new DeltaSharing();
        instance.profileProvider = profileProvider;
        instance.httpClient = new DeltaSharingRestClient(profileProvider, 120, 4, false);
        instance.checkpointPath = checkpointPath;
        instance.metadataMap = new HashMap<>();
        if (!Files.exists(checkpointPath)) {
            Files.createDirectory(instance.checkpointPath);
        }
        instance.tempDir = Files.createTempDirectory(instance.checkpointPath, "delta_sharing");
        instance.tempDir.toFile().deleteOnExit();
        return instance;
    }

    public static DeltaSharing create(String providerConf, String checkpointLocation) throws IOException {
        Path checkpointPath = Paths.get(checkpointLocation);
        if(!Files.exists(checkpointPath)){
            Files.createDirectory(checkpointPath);
        }
        DeltaSharingProfileProvider profileProvider = new DeltaSharingJSONProvider(providerConf);
        return create(profileProvider, checkpointPath);
    }
}
