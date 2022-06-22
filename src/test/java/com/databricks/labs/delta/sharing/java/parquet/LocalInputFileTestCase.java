package com.databricks.labs.delta.sharing.java.parquet;

import com.databricks.labs.delta.sharing.java.format.parquet.LocalInputFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test cases for local Input File.
 */
public class LocalInputFileTestCase {

  @Test
  public void testRead() throws IOException {
    Path filePath =
        Paths.get("src", "test", "resources", "table_reader_test_data.parquet").toAbsolutePath();
    LocalInputFile localInputFile = new LocalInputFile(filePath);

    byte[] b = new byte[1024];
    SeekableInputStream stream = localInputFile.newStream();

    Assertions.assertDoesNotThrow(() -> stream.read(b), "assert no exception for read with buffer");

    Assertions.assertDoesNotThrow(() -> stream.read(),
        "assert no exception for read without buffer");

    Assertions.assertDoesNotThrow(() -> stream.read(b, 100, 100),
        "assert no exception for read with offset using a buffer");

    Assertions.assertDoesNotThrow(() -> stream.readFully(b, 100, 100),
        "assert no exception for read with offset using a buffer");
  }

  @Test
  public void testSkip() throws IOException {
    Path filePath =
        Paths.get("src", "test", "resources", "table_reader_test_data.parquet").toAbsolutePath();
    LocalInputFile localInputFile = new LocalInputFile(filePath);

    byte[] b = new byte[1024];
    SeekableInputStream stream = localInputFile.newStream();

    Assertions.assertDoesNotThrow(() -> stream.skip(100) + stream.read(b),
        "assert no exception for read after a skip");
  }

  @Test
  public void testReadByteBuffers() throws IOException {
    Path filePath =
        Paths.get("src", "test", "resources", "table_reader_test_data.parquet").toAbsolutePath();
    LocalInputFile localInputFile = new LocalInputFile(filePath);

    // 8192 is page size constant inside the LocalInputFile
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8192 * 4);
    SeekableInputStream stream = localInputFile.newStream();

    Assertions.assertDoesNotThrow(stream::reset, "assert no exception for reset");

    Assertions.assertDoesNotThrow(() -> stream.read(byteBuffer),
        "assert no exception for read with byte buffer");

    Assertions.assertDoesNotThrow(() -> stream.readFully(byteBuffer),
        "assert no exception for readFully with byte buffer");
  }

  @Test
  public void testPointers() throws IOException {
    Path filePath =
        Paths.get("src", "test", "resources", "table_reader_test_data.parquet").toAbsolutePath();
    LocalInputFile localInputFile = new LocalInputFile(filePath);

    // 8192 is page size constant inside the LocalInputFile
    SeekableInputStream stream = localInputFile.newStream();

    Assertions.assertTrue(stream.markSupported());
    Assertions.assertEquals(stream.available(), 0);

    Assertions.assertDoesNotThrow(() -> stream.mark(0),
        "assert mark operation doesnt throw an exception");

    Assertions.assertDoesNotThrow(() -> stream.mark(0),
        "assert mark operation doesnt throw an exception");
  }
}
