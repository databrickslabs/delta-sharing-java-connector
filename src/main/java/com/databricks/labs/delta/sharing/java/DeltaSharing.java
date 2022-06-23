package com.databricks.labs.delta.sharing.java;

import com.databricks.labs.delta.sharing.java.adaptor.DeltaSharingJsonProvider;
import com.databricks.labs.delta.sharing.java.format.parquet.TableReader;
import io.delta.sharing.spark.DeltaSharingFileSystem;
import io.delta.sharing.spark.DeltaSharingProfileProvider;
import io.delta.sharing.spark.DeltaSharingRestClient;
import io.delta.sharing.spark.InMemoryHttpInputStream;
import io.delta.sharing.spark.model.AddFile;
import io.delta.sharing.spark.model.DeltaTableFiles;
import io.delta.sharing.spark.model.DeltaTableMetadata;
import io.delta.sharing.spark.model.Table;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import scala.Option$;
import scala.Some$;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A wrapper class for {@link io.delta.sharing.spark.DeltaSharingRestClient}
 * instance. This class ensures we have access to a temp directory where parquet
 * files will be kept during the life of the JVM. The temp directory has a
 * destroy hook register as do all the files when they land in the temp
 * directory. The class keeps a runtime map of metadata for each file stored in
 * the temp directory. If the metadata has changed we will retrieve the new
 * state of the file. If the metadata has remained the same we will use the
 * available local copy in the temp directory.
 * <p/>
 *
 * @since 0.1.0
 */
public class DeltaSharing {
  private final DeltaSharingProfileProvider profileProvider;
  private final DeltaSharingRestClient httpClient;
  private final Path checkpointPath;
  private final Path tempDir;
  private final Map<String, DeltaTableMetadata> metadataMap;

  /**
   * Getter for {@link DeltaSharing#profileProvider}.
   */
  public DeltaSharingProfileProvider getProfileProvider() {
    return profileProvider;
  }

  /**
   * Getter for {@link DeltaSharing#httpClient}.
   */
  public DeltaSharingRestClient getHttpClient() {
    return httpClient;
  }

  /**
   * Getter for checkpointPath.
   */
  public Path getCheckpointPath() {
    return checkpointPath;
  }

  /**
   * Constructor.
   *
   * @param profileProvider An instance of {@link DeltaSharingProfileProvider}.
   * @param checkpointPath  An path to a temporary checkpoint location.
   * @throws IOException Transitive due to the call to
   *                     {@link Files#createTempDirectory(String, FileAttribute[])}.
   */
  public DeltaSharing(final DeltaSharingProfileProvider profileProvider,
                      final Path checkpointPath) throws IOException {

    if (!Files.exists(checkpointPath)) {
      Files.createDirectory(checkpointPath);
    }
    Path tempDir = Files.createTempDirectory(checkpointPath, "delta_sharing");
    tempDir.toFile().deleteOnExit();

    this.profileProvider = profileProvider;
    this.httpClient =
        new DeltaSharingRestClient(profileProvider, 120, 4, false);
    this.checkpointPath = checkpointPath;
    this.tempDir = tempDir;
    this.metadataMap = new HashMap<>();
  }

  /**
   * Constructor.
   *
   * @param providerConf       A valid JSON document corresponding to
   *                           {@link DeltaSharingProfileProvider}.
   * @param checkpointLocation A string containing a path to be used as a
   *                           checkpoint location.
   * @throws IOException Transitive due to the call to
   *                     {@link Files#createDirectories(Path, FileAttribute[])}.
   */
  public DeltaSharing(final String providerConf,
                      final String checkpointLocation) throws IOException {
    this(new DeltaSharingJsonProvider(providerConf),
        Paths.get(checkpointLocation));
  }


  /**
   * Adapter method for getting a List of all tables. Scala API returns a
   * {@link Seq} and we require a {@link List}.
   *
   * @return A list of all tables.
   * @implNote Suppress unnecessary local variable is done to remove warnings
   * for a decoupled Scala to Java conversion call and a return call.
   */
  @SuppressWarnings("UnnecessaryLocalVariable")
  public List<Table> listAllTables() {
    Seq<Table> tables = httpClient.listAllTables();
    List<Table> tableList = JavaConverters.seqAsJavaList(tables);
    return tableList;
  }

  /**
   * Getter for
   * {@link io.delta.sharing.spark.DeltaSharingRestClient#getMetadata(Table)}.
   *
   * @return DeltaTableMetadata
   */
  public DeltaTableMetadata getMetadata(Table table) {
    return httpClient.getMetadata(table);
  }

  /**
   * Getter for
   * {@link io.delta.sharing.spark.DeltaSharingRestClient#getTableVersion(Table)}
   * (Table)}.
   */
  public long getTableVersion(Table table) {
    return httpClient.getTableVersion(table);
  }

  /**
   * Adapter method for getting a List of files belonging to a {@link Table}.
   * Scala API returns a {@link Seq} and we require a {@link List}.
   *
   * @return A list of files corresponding to a table.
   * @implNote Suppress unnecessary local variable is done to remove warnings
   * for a decoupled Scala to Java conversion call and a return call.
   */
  @SuppressWarnings("UnnecessaryLocalVariable")
  public List<AddFile> getFiles(Table table, List<String> predicates,
                                Integer limit) {
    Seq<String> predicatesSeq = JavaConverters
        .asScalaIteratorConverter(predicates.iterator()).asScala().toSeq();
    DeltaTableFiles deltaTableFiles;
    if (limit != null) {
      deltaTableFiles =
          httpClient.getFiles(table, predicatesSeq, Some$.MODULE$.apply(limit));
    } else {
      deltaTableFiles = httpClient.getFiles(table, predicatesSeq,
          Option$.MODULE$.apply(null));
    }
    List<AddFile> files = JavaConverters.seqAsJavaList(deltaTableFiles.files());
    return files;
  }

  /**
   * Adapter method for getting a List of files belonging to a {@link Table}.
   * Scala API returns a {@link Seq} and we require a {@link List}.
   *
   * @return A list of files corresponding to a table.
   * @implNote Suppress unnecessary local variable is done to remove warnings
   * for a decoupled Scala to Java conversion call and a return call.
   */
  public List<AddFile> getFiles(Table table, List<String> predicates) {
    return getFiles(table, predicates, null);
  }

  /**
   * Adapter method for getting table coordinates.
   *
   * @return A string representing coordinates of the table.
   */
  public String getCoordinates(Table table) {
    Pattern pattern = Pattern.compile(
        "[a-zA-Z0-9\\-_]*\\.[a-zA-Z0-9\\-_]*\\.[a-zA-Z0-9\\-_]*",
        Pattern.CASE_INSENSITIVE);
    String coords = table.share() + "." + table.schema() + "." + table.name();
    Matcher matcher = pattern.matcher(coords);
    boolean matchFound = matcher.find();
    if (!matchFound) {
      throw new IllegalArgumentException("Invalid format for coordinates");
    }

    return coords;
  }

  /**
   * Getter for a temp file that will be stored in
   * {@link DeltaSharing#checkpointPath}.
   *
   * @param file File for which we are generating the checkpoint path for.
   * @return A fully qualified path for a checkpoint file copy.
   */
  private Path getFileCheckpointPath(AddFile file) {
    String fileId = file.id();
    Pattern pattern = Pattern.compile("[a-zA-Z0-9]*", Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(fileId);
    boolean matchFound = matcher.find();
    if (!matchFound) {
      throw new IllegalArgumentException(
          "Invalid format for file id. The id contains special characters.");
    }

    String path = String.format("%s/%s.parquet", this.tempDir, file.id());
    return Paths.get(path);
  }

  /**
   * Fetches the remote files as input streams and writes the content into a
   * {@link DeltaSharing#checkpointPath}.
   *
   * @param files Files for which we are generating the checkpoint file copies.
   * @return A fully qualified path for a checkpoint file copy.
   * @throws IOException Transitive exception due to the call to
   *                     {@link Files#write(Path, byte[], OpenOption...)}.
   */
  private List<Path> writeCheckpointFiles(List<AddFile> files)
      throws IOException, URISyntaxException {
    List<Path> paths = new LinkedList<>();
    for (AddFile file : files) {
      FSDataInputStream stream = new FSDataInputStream(
          new InMemoryHttpInputStream(new URI(file.url())));
      Path path = getFileCheckpointPath(file);
      paths.add(path);
      Files.write(path, IOUtils.toByteArray(stream));
      path.toFile().deleteOnExit();
    }
    return paths;
  }

  /**
   * Getter for a temp files that will be stored in
   * {@link DeltaSharing#checkpointPath}.
   *
   * @param files Files for which we are generating the checkpoint paths for.
   * @return A List of fully qualified paths for a checkpoint file copies.
   */
  private List<Path> getCheckpointPaths(List<AddFile> files) {
    List<Path> paths = new LinkedList<>();
    for (AddFile file : files) {
      Path path = getFileCheckpointPath(file);
      paths.add(path);
    }
    return paths;
  }

  /**
   * Resolves and constructs the {@link TableReader} instance associated with
   * the table. It inspects the available {@link DeltaSharing#metadataMap} and
   * based on the metadata it re-fetches the stale into the
   * {@link DeltaSharing#checkpointPath} directory.
   *
   * @param table Table whose reader is requested.
   * @return An instance of {@link TableReader} that will manage the reads from
   * the table.
   * @throws IOException Transitive due to the call to
   *                     {@link TableReader#TableReader(List)}.
   */
  @SuppressWarnings("UnnecessaryLocalVariable")
  public TableReader<GenericRecord> getTableReader(Table table)
      throws IOException, URISyntaxException {
    List<AddFile> files = getFiles(table, new LinkedList<>());
    try (DeltaSharingFileSystem fs = new DeltaSharingFileSystem()) {
      fs.setConf(new Configuration());
    }
    String uniqueRef = getCoordinates(table);
    DeltaTableMetadata newMetadata = this.getMetadata(table);

    List<Path> paths = getPaths(uniqueRef, files, newMetadata);

    TableReader<GenericRecord> tableReader = new TableReader<>(paths);
    return tableReader;
  }

  /**
   * Fetches the list of file pats from the checkpoint location.
   * Files whose metadata has drifted are updated.
   *
   * @param uniqueRef   Reference via table coordinates.
   * @param files       A list of add files for the table.
   * @param newMetadata A new value of the metadata for the table.
   * @return A list of paths to checkpoint files.
   * @throws IOException        Read/Write errors can occur if temp directory has been altered outside the JVM.
   * @throws URISyntaxException URI exception can be thrown by writeCheckpointFiles method call.
   */
  private List<Path> getPaths(String uniqueRef, List<AddFile> files, DeltaTableMetadata newMetadata) throws IOException, URISyntaxException {
    List<Path> paths;
    if (this.metadataMap.containsKey(uniqueRef)) {
      DeltaTableMetadata metadata = this.metadataMap.get(uniqueRef);
      if (!newMetadata.equals(metadata)) {
        paths = writeCheckpointFiles(files);
        this.metadataMap.put(uniqueRef, newMetadata);
      } else {
        paths = getCheckpointPaths(files);
      }
    } else {
      paths = writeCheckpointFiles(files);
      this.metadataMap.put(uniqueRef, newMetadata);
    }
    return paths;
  }

  /**
   * A reader method that reads all the records from all the files that belong
   * to a table.
   *
   * @param table An instance of {@link Table} whose records we are reading.
   * @return A list of records from the table instance.
   * @throws IOException Transitive due to the call to
   *                     {@link TableReader#read()}
   */
  public List<GenericRecord> getAllRecords(Table table)
      throws IOException, URISyntaxException {
    TableReader<GenericRecord> tableReader = getTableReader(table);
    List<GenericRecord> records = new LinkedList<>();
    GenericRecord currentRecord = tableReader.read();
    while (currentRecord != null) {
      records.add(currentRecord);
      currentRecord = tableReader.read();
    }
    return records;
  }

  /**
   * A reader method that reads all the records from all the files that belong
   * to a table. This call will always create a new instance of the reader and
   * will always return the same N records. For full reads use
   * {@link DeltaSharing#getTableReader(Table)} to access the reader and then
   * use {@link TableReader#readN(Integer)} to read blocks of records.
   *
   * @param table  An instance of {@link Table} whose records we are reading.
   * @param numRec Number of records to be read at most.
   * @return A list of records from the table instance. If less records are
   * available, only the available records will be returned.
   * @throws IOException Transitive due to the call to
   *                     {@link TableReader#read()}
   */
  public List<GenericRecord> getNumRecords(Table table, int numRec)
      throws IOException, URISyntaxException {
    TableReader<GenericRecord> tableReader = getTableReader(table);
    return tableReader.readN(numRec);
  }
}
