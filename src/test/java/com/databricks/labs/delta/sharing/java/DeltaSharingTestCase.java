package com.databricks.labs.delta.sharing.java;

import com.databricks.labs.delta.sharing.java.adaptor.DeltaSharingJsonProvider;
import com.databricks.labs.delta.sharing.java.format.parquet.TableReader;
import com.databricks.labs.delta.sharing.java.mocks.Mocks;
import io.delta.sharing.spark.DeltaSharingProfileProvider;
import io.delta.sharing.spark.model.Table;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test cases for Delta Sharing connector.
 *
 */
public class DeltaSharingTestCase {

  @Test
  public void testGetters() throws IOException {
    DeltaSharingProfileProvider profileProvider =
        new DeltaSharingJsonProvider(Mocks.providerJson);
    Path checkpointPath = Paths.get("target/testing/");
    DeltaSharing sharing = new DeltaSharing(profileProvider, checkpointPath);
    Assertions.assertAll("assert sharing client",
        () -> Assertions.assertNotNull(sharing.getHttpClient()),
        () -> Assertions.assertNotNull(sharing.getProfileProvider()),
        () -> Assertions.assertNotNull(sharing.getCheckpointPath()));
  }

  @Test
  public void testGetterStringConstructor() throws IOException {
    DeltaSharing sharing =
        new DeltaSharing(Mocks.providerJson, "target/testing/");
    Assertions.assertAll("assert sharing client",
        () -> Assertions.assertNotNull(sharing.getHttpClient()),
        () -> Assertions.assertNotNull(sharing.getProfileProvider()),
        () -> Assertions.assertNotNull(sharing.getCheckpointPath()));
  }

  @Test
  public void testListAllTables() throws IOException {
    DeltaSharingProfileProvider profileProvider =
        new DeltaSharingJsonProvider(Mocks.providerJson);
    Path checkpointPath = Paths.get("target/testing/");
    DeltaSharing sharing = new DeltaSharing(profileProvider, checkpointPath);
    List<Table> tables = sharing.listAllTables();
    Assertions.assertTrue(tables.size() > 0);
  }

  @Test
  public void testTableOperations() throws IOException {
    DeltaSharingProfileProvider profileProvider =
        new DeltaSharingJsonProvider(Mocks.providerJson);
    Path checkpointPath = Paths.get("target/testing/");
    DeltaSharing sharing = new DeltaSharing(profileProvider, checkpointPath);
    List<Table> tables = sharing.listAllTables();
    Table firstTable = tables.get(0);

    Assertions.assertNotNull(sharing.getMetadata(firstTable));
    Assertions.assertEquals(0, sharing.getTableVersion(firstTable));
    Assertions.assertNotNull(sharing.getFiles(firstTable, new LinkedList<>()));
  }

  @Test
  public void testReadAll() throws IOException, URISyntaxException {
    DeltaSharing sharing =
        new DeltaSharing(Mocks.providerJson, "target/testing/");
    List<Table> tables = sharing.listAllTables();
    Table firstTable = tables.get(0);

    Assertions.assertTrue(sharing.getAllRecords(firstTable).size() > 0);
    Assertions.assertNotNull(sharing.getAllRecords(firstTable));
    Assertions.assertNotNull(sharing.getNumRecords(firstTable, 100));
  }

  @Test
  public void testReadMoreThanFileRows() throws IOException, URISyntaxException {
    DeltaSharing sharing =
        new DeltaSharing(Mocks.providerJson, "target/testing/");
    List<Table> tables = sharing.listAllTables();
    Table firstTable = tables.get(0);
    TableReader<GenericRecord> reader = sharing.getTableReader(firstTable);
    GenericRecord current = reader.read();
    while (current != null) {
      current = reader.read();
    }
    Assertions.assertNull(reader.read());
    Assertions.assertEquals(reader.readN(5).size(), 0);

  }
}
