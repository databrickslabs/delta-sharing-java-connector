package io.delta.sharing.java;


import io.delta.sharing.java.adaptor.DeltaSharingJsonProvider;
import io.delta.sharing.java.mocks.Mocks;
import io.delta.sharing.spark.DeltaSharingProfileProvider;
import io.delta.sharing.spark.model.Table;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test cases for Delta Sharing connector.
 *
 */
public class DeltaSharingTestCase {

  @Test
  public void testGetters() throws IOException {
    DeltaSharingProfileProvider profileProvider = new DeltaSharingJsonProvider(Mocks.providerJson);
    Path checkpointPath = Paths.get("target/testing/");
    DeltaSharing sharing = DeltaSharingFactory.create(profileProvider, checkpointPath);
    Assertions.assertAll("assert sharing client",
        () -> Assertions.assertNotNull(sharing.getHttpClient()),
        () -> Assertions.assertNotNull(sharing.getProfileProvider()));
  }

  @Test
  public void testListAllTables() throws IOException {
    DeltaSharingProfileProvider profileProvider = new DeltaSharingJsonProvider(Mocks.providerJson);
    Path checkpointPath = Paths.get("target/testing/");
    DeltaSharing sharing = DeltaSharingFactory.create(profileProvider, checkpointPath);
    List<Table> tables = sharing.listAllTables();
    Assertions.assertTrue(tables.size() > 0);
  }

  @Test
  public void testTableOperations() throws IOException {
    DeltaSharing sharing = DeltaSharingFactory.create(Mocks.providerJson, "target/testing/");
    List<Table> tables = sharing.listAllTables();
    Table firstTable = tables.get(0);

    Assertions.assertNotNull(sharing.getMetadata(firstTable));
    Assertions.assertEquals(0, sharing.getTableVersion(firstTable));
    Assertions.assertNotNull(sharing.getFiles(firstTable, new LinkedList<>()));
  }

  @Test
  public void testReadAll() throws IOException, URISyntaxException {
    DeltaSharing sharing = DeltaSharingFactory.create(Mocks.providerJson, "target/testing/");
    List<Table> tables = sharing.listAllTables();
    Table firstTable = tables.get(0);

    Assertions.assertTrue(sharing.getAllRecords(firstTable).size() > 0);
    Assertions.assertNotNull(sharing.getAllRecords(firstTable));
    Assertions.assertNotNull(sharing.getNumRecords(firstTable, 100));
  }
}
