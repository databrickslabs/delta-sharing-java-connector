package com.databricks.labs.delta.sharing.java.format.parquet;

import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * A reader class for a delta sharing table. It contains a queue of
 * {@link ParquetReader} instances. This way we abstract a number of files that
 * constitute a table.
 * <p/>
 *
 * @since 0.1.0
 */
public class TableReader<T> {
  private Queue<ParquetReader<T>> readers;
  private final List<Path> paths;

  /**
   * Constructor.
   *
   * @param paths A list of paths of parquet files that constitute a table. An
   *              individual instance of {@link ParquetReader} will be created for
   *              each file.
   * @throws IOException Transitive throws due to
   *                     {@link TableReader#reinitialize()} call.
   */
  public TableReader(List<Path> paths) throws IOException {
    this.paths = paths;
    this.reinitialize();
  }

  /**
   * Reads a single record from first non-empty {@link ParquetReader} instance.
   * The method is NOT idempotent. When the current reader in the queue is empty
   * the queue will pop the reader and move to the next available file.
   *
   * @return An instance of template type if any is available in the available
   * non-empty readers. If the queue is empty it returns null.
   * @throws IOException Transitive throws due to {@link ParquetReader#read()}
   *                     call.
   * @implNote The method is recursive. Recursion is used to abstract from
   * rotation of readers in the queue.
   */
  public T read() throws IOException {
    ParquetReader<T> currentReader = this.readers.peek();
    if (currentReader == null) {
      return null;
    }

    T currentRecord = currentReader.read();
    if (currentRecord != null) {
      return currentRecord;
    }

    this.readers.poll();
    return read();
  }

  /**
   * Reads num records from first non-empty {@link ParquetReader} instances. If
   * there are fewer than num records available it will return K available
   * records. The method is NOT idempotent. When the current reader in the queue
   * is empty the queue will pop the reader and move to the next available file.
   *
   * @return A list of num instances of template type if any is available in the
   * available non-empty readers. If the queue is empty before N records
   * were fetched it will return the available records. If called on an
   * empty reader it will return an empty list.
   * @throws IOException Transitive throws due to {@link TableReader#read()}
   *                     call.
   */
  public List<T> readN(Integer num) throws IOException {
    List<T> records = new LinkedList<>();
    for (int i = 0; i < num; i++) {
      T current = read();
      if (current == null) {
        break;
      }
      records.add(current);
    }
    return records;
  }

  /**
   * Constructs the reader instances based on the {@link TableReader#paths}
   * param.
   *
   * @return The list containing the {@link ParquetReader} instances that will
   * constitute the runtime queue.
   * @throws IOException Transitive due to a call to
   *                     {@link LocalInputFile#LocalInputFile(Path)} constructor.
   */
  private List<ParquetReader<T>> getReaders() throws IOException {
    List<ParquetReader<T>> readers = new LinkedList<>();
    for (Path path : paths) {
      LocalInputFile localInputFile = new LocalInputFile(path);
      ParquetReader<T> reader =
          AvroParquetReader.<T>builder(localInputFile).build();
      readers.add(reader);
    }
    return readers;
  }

  /**
   * Restates the table reader. This is needed due to the fact that reads are
   * NOT idempotent.
   *
   * @throws IOException Transitive due to a call to
   *                     {@link TableReader#getReaders()}.
   */
  public void reinitialize() throws IOException {
    this.readers = new LinkedList<>();
    this.readers.addAll(getReaders());
  }
}
