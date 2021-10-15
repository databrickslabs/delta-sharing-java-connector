package io.delta.sharing.java;

import io.delta.sharing.java.format.parquet.TableReader;
import io.delta.sharing.java.util.FileStreamUtil;
import io.delta.sharing.spark.DeltaSharingFileSystem;
import io.delta.sharing.spark.DeltaSharingProfileProvider;
import io.delta.sharing.spark.DeltaSharingRestClient;
import io.delta.sharing.spark.model.AddFile;
import io.delta.sharing.spark.model.DeltaTableFiles;
import io.delta.sharing.spark.model.DeltaTableMetadata;
import io.delta.sharing.spark.model.Table;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import scala.Option$;
import scala.Some$;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DeltaSharing {
    DeltaSharingProfileProvider profileProvider;
    DeltaSharingRestClient httpClient;
    Path checkpointPath;
    Path tempDir;
    Map<String, DeltaTableMetadata> metadataMap;

    public DeltaSharing(DeltaSharingProfileProvider profileProvider, Path checkpointPath) throws IOException {
        this.profileProvider = profileProvider;
        this.httpClient = new DeltaSharingRestClient(profileProvider, 120, 4, false);
        this.checkpointPath = checkpointPath;
        this.metadataMap = new HashMap<>();
        if (!Files.exists(checkpointPath)) {
            Files.createDirectory(this.checkpointPath);
        }
        this.tempDir = Files.createTempDirectory(this.checkpointPath, "delta_sharing");
        this.tempDir.toFile().deleteOnExit();
    }

    public DeltaSharingProfileProvider getProfileProvider() {
        return profileProvider;
    }

    public DeltaSharingRestClient getHttpClient() {
        return httpClient;
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    public List<Table> listAllTables() {
        Seq<Table> tables = httpClient.listAllTables();
        List<Table> tableList = JavaConverters.seqAsJavaList(tables);
        return tableList;
    }

    public DeltaTableMetadata getMetadata(Table table) {
        return httpClient.getMetadata(table);
    }

    public long getTableVersion(Table table) {
        return httpClient.getTableVersion(table);
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    public List<AddFile> getFiles(Table table,
                                  List<String> predicates,
                                  Integer limit) {
        Seq<String> predicatesSeq = JavaConverters.asScalaIteratorConverter(predicates.iterator()).asScala().toSeq();
        DeltaTableFiles deltaTableFiles;
        if (limit != null) {
            deltaTableFiles = httpClient.getFiles(table, predicatesSeq, Some$.MODULE$.apply(limit));
        } else {
            deltaTableFiles = httpClient.getFiles(table, predicatesSeq, Option$.MODULE$.apply(null));
        }
        List<AddFile> files = JavaConverters.seqAsJavaList(deltaTableFiles.files());
        return files;
    }

    public List<AddFile> getFiles(Table table,
                                    List<String> predicates) {
        return getFiles(table, predicates, null);
    }

    public String getCoordinates(Table table) {
        return table.share() + "." + table.schema() + "." + table.name();
    }

    private Path getFileCheckpointPath(AddFile file) {
        String path = String.format("%s/%s.parquet", this.tempDir, file.id());
        return Paths.get(path);
    }

    private List<Path> writeCheckpointFiles(List<AddFile> files, DeltaSharingFileSystem fs) throws IOException {
        List<Path> paths = new LinkedList<>();
        for (AddFile file : files) {
            FSDataInputStream stream = fs.open(DeltaSharingFileSystem.createPath(URI.create(file.url()), file.size()), 1024);
            Path path = getFileCheckpointPath(file);
            paths.add(path);
            Files.write(path, FileStreamUtil.readAllBytes(stream));
            path.toFile().deleteOnExit();
        }
        return paths;
    }

    private List<Path> getCheckpointPaths(List<AddFile> files) {
        List<Path> paths = new LinkedList<>();
        for (AddFile file : files) {
            Path path = getFileCheckpointPath(file);
            paths.add(path);
        }
        return paths;
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    public TableReader<GenericRecord> getTableReader(Table table) throws IOException {
        List<AddFile> files = getFiles(table, new LinkedList<>());
        DeltaSharingFileSystem fs = new DeltaSharingFileSystem();
        fs.setConf(new Configuration());
        String uniqueRef = getCoordinates(table);
        List<Path> paths;
        DeltaTableMetadata newMetadata = this.getMetadata(table);
        if (this.metadataMap.containsKey(uniqueRef)) {
            DeltaTableMetadata metadata = this.metadataMap.get(uniqueRef);
            if (!newMetadata.equals(metadata)) {
                paths = writeCheckpointFiles(files, fs);
                this.metadataMap.put(uniqueRef, newMetadata);
            } else {
                paths = getCheckpointPaths(files);
            }
        } else {
            paths = writeCheckpointFiles(files, fs);
            this.metadataMap.put(uniqueRef, newMetadata);
        }

        TableReader<GenericRecord> tableReader = new TableReader<>(paths);
        return tableReader;
    }

    public List<GenericRecord> getAllRecords(Table table) throws IOException {
        TableReader<GenericRecord> tableReader = getTableReader(table);
        List<GenericRecord> records = new LinkedList<>();
        GenericRecord currentRecord = tableReader.read();
        while(currentRecord != null) {
            records.add(currentRecord);
            currentRecord = tableReader.read();
        }
        return records;
    }
}
