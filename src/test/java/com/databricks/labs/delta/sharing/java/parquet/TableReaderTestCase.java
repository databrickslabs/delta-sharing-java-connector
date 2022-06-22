package com.databricks.labs.delta.sharing.java.parquet;

import com.databricks.labs.delta.sharing.java.format.parquet.TableReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test cases for Table Reader.
 */
public class TableReaderTestCase {

  @Test
  public void testRead() throws IOException {
    List<Path> paths = new LinkedList<>();
    paths.add(
        Paths.get("src", "test", "resources", "table_reader_test_data.parquet").toAbsolutePath());

    TableReader<GenericRecord> tableReader = new TableReader<>(paths);

    GenericRecord record = tableReader.read();
    Schema schema = record.getSchema();
    Schema.Field field = schema.getFields().get(0);
    Assertions.assertAll("assert table reader",
        () -> Assertions.assertEquals(record.get("date").toString(), "2020-10-10"),
        () -> Assertions.assertEquals(record.get(field.name()).toString(), "2020-10-10"));
  }

  @Test
  public void testReadN() throws IOException {
    List<Path> paths = new LinkedList<>();
    paths.add(
        Paths.get("src", "test", "resources", "table_reader_test_data.parquet").toAbsolutePath());

    TableReader<GenericRecord> tableReader = new TableReader<>(paths);

    List<GenericRecord> records = tableReader.readN(2);
    Schema schema = records.get(0).getSchema();
    Schema.Field field = schema.getFields().get(0);
    Assertions.assertAll("assert table reader",
        () -> Assertions.assertEquals(records.get(0).get("date").toString(), "2020-10-10"),
        () -> Assertions.assertEquals(records.get(0).get(field.name()).toString(), "2020-10-10"));
  }
}
