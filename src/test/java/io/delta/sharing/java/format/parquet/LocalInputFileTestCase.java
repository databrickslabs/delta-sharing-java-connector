package io.delta.sharing.java.format.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class LocalInputFileTestCase {

    @Test
    public void testRead() throws IOException {
        Path filePath = Paths.get("src", "test", "resources", "table_reader_test_data.parquet").toAbsolutePath();
        LocalInputFile localInputFile = new LocalInputFile(filePath);

        byte[] b = new byte[1024];
        SeekableInputStream stream = localInputFile.newStream();

        Assertions.assertDoesNotThrow(
                () -> stream.read(b),
                "assert no exception for read with buffer"
        );

        Assertions.assertDoesNotThrow(
                () -> stream.read(),
                "assert no exception for read without buffer"
        );

        Assertions.assertDoesNotThrow(
                () -> stream.read(b, 100, 100),
                "assert no exception for read with offset using a buffer"
        );
    }

    @Test
    public void testSkip() throws IOException {
        Path filePath = Paths.get("src", "test", "resources", "table_reader_test_data.parquet").toAbsolutePath();
        LocalInputFile localInputFile = new LocalInputFile(filePath);

        byte[] b = new byte[1024];
        SeekableInputStream stream = localInputFile.newStream();

        Assertions.assertDoesNotThrow(
                () -> stream.skip(100) + stream.read(b),
                "assert no exception for read after a skip"
        );
    }

    @Test
    public void testReadByteBuffers() throws IOException {
        Path filePath = Paths.get("src", "test", "resources", "table_reader_test_data.parquet").toAbsolutePath();
        LocalInputFile localInputFile = new LocalInputFile(filePath);


        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
        SeekableInputStream stream = localInputFile.newStream();

        Assertions.assertDoesNotThrow(
                () -> stream.read(byteBuffer),
                "assert no exception for read with byte buffer"
        );

        Assertions.assertDoesNotThrow(
                () -> stream.readFully(byteBuffer),
                "assert no exception for readFully with byte buffer"
        );
    }
}
