package io.delta.sharing.java;

import io.delta.sharing.java.adaptor.DeltaSharingJSONProvider;
import io.delta.sharing.java.mocks.Mocks;
import io.delta.sharing.spark.DeltaSharingProfileProvider;
import io.delta.sharing.spark.model.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class DeltaSharingTestCase {

    @Test
    public void testGetters() throws IOException {
        DeltaSharingProfileProvider profileProvider = new DeltaSharingJSONProvider(Mocks.providerJSON);
        Path checkpointPath = Paths.get("/Users/milos.colic/IdeaProjects/DeltaSharingJavaConnector/target/testing/");
        DeltaSharing sharing = DeltaSharingFactory.create(profileProvider, checkpointPath);
        Assertions.assertAll(
            "assert sharing client",
            () ->
                Assertions.assertNotNull(sharing.getHttpClient()),
            () ->
                Assertions.assertNotNull(sharing.getProfileProvider())
        );
    }

    @Test
    public void testListAllTables() throws IOException {
        DeltaSharingProfileProvider profileProvider = new DeltaSharingJSONProvider(Mocks.providerJSON);
        Path checkpointPath = Paths.get("/Users/milos.colic/IdeaProjects/DeltaSharingJavaConnector/target/testing/");
        DeltaSharing sharing = DeltaSharingFactory.create(profileProvider, checkpointPath);
        List<Table> tables = sharing.listAllTables();
        Assertions.assertTrue(tables.size() > 0);
    }

    @Test
    public void testTableOperations() throws IOException {
        DeltaSharing sharing = DeltaSharingFactory.create(Mocks.providerJSON, "/Users/milos.colic/IdeaProjects/DeltaSharingJavaConnector/target/testing/");
        List<Table> tables = sharing.listAllTables();
        Table firstTable = tables.get(0);

        Assertions.assertNotNull(sharing.getMetadata(firstTable));
        Assertions.assertEquals(0, sharing.getTableVersion(firstTable));
        Assertions.assertNotNull(sharing.getFiles(firstTable, new LinkedList<>()));
    }

    @Test
    public void testReadAll() throws IOException {
        DeltaSharing sharing = DeltaSharingFactory.create(Mocks.providerJSON, "/Users/milos.colic/IdeaProjects/DeltaSharingJavaConnector/target/testing/");
        List<Table> tables = sharing.listAllTables();
        Table firstTable = tables.get(0);

        Assertions.assertTrue(sharing.getAllRecords(firstTable).size() > 0);
        Assertions.assertNotNull(sharing.getAllRecords(firstTable));
        Assertions.assertNotNull(sharing.getNRecords(firstTable, 100));
    }
}
