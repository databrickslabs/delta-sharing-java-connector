package io.delta.sharing.java.format.parquet;

import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class TableReader<T> {
    Queue<ParquetReader<T>> readers;

    public TableReader(List<Path> paths) throws IOException {
        List<ParquetReader<T>> readers = new LinkedList<>();
        for (Path path : paths) {
            LocalInputFile localInputFile = new LocalInputFile(path);
            ParquetReader<T> reader = AvroParquetReader.<T>builder(localInputFile).build();
            readers.add(reader);
        }
        this.readers = new LinkedList<>();
        this.readers.addAll(readers);
    }

    public T read() throws IOException {
        ParquetReader<T> currentReader = this.readers.peek();
        if( currentReader != null) {
            T currentRecord = currentReader.read();
            if (currentRecord != null) {
                return currentRecord;
            } else {
                this.readers.poll();
                return read();
            }
        } else {
            return null;
        }
    }
}
