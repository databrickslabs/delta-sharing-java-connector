package io.delta.sharing.java;

import io.delta.sharing.java.adaptor.DeltaSharingJsonProvider;
import io.delta.sharing.spark.DeltaSharingProfileProvider;
import io.delta.sharing.spark.DeltaSharingRestClient;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.HashMap;

/**
 * Factory class for {@link DeltaSharing}. It provides different constructors that might be
 * appropriate in different situations.
 */
public class DeltaSharingFactory {

  /**
   * Constructor.
   *
   * @param profileProvider An instance of {@link DeltaSharingProfileProvider}.
   * @param checkpointPath An path to a temporary checkpoint location.
   * @return An instance of {@link DeltaSharing} client.
   * @throws IOException Transitive due to the call to
   *         {@link Files#createTempDirectory(String, FileAttribute[])}.
   */
  public static DeltaSharing create(DeltaSharingProfileProvider profileProvider,
      Path checkpointPath) throws IOException {
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

  /**
   * Constructor.
   *
   * @param providerConf A valid JSON document corresponding to {@link DeltaSharingProfileProvider}.
   * @param checkpointLocation A string containing a path to be used as a checkpoint location.
   * @return An instance of {@link DeltaSharing} client.
   * @throws IOException Transitive due to the call to
   *         {@link Files#createDirectories(Path, FileAttribute[])}.
   */
  public static DeltaSharing create(String providerConf, String checkpointLocation)
      throws IOException {
    Path checkpointPath = Paths.get(checkpointLocation);
    if (!Files.exists(checkpointPath)) {
      Files.createDirectories(checkpointPath);
    }
    DeltaSharingProfileProvider profileProvider = new DeltaSharingJsonProvider(providerConf);
    return create(profileProvider, checkpointPath);
  }
}
