package io.delta.sharing.java;

import io.delta.sharing.spark.DeltaSharingFileSystem;
import io.delta.sharing.spark.DeltaSharingFileSystem$;
import io.delta.sharing.spark.DeltaSharingProfileProvider;
import io.delta.sharing.spark.DeltaSharingRestClient;
import io.delta.sharing.spark.model.AddFile;
import io.delta.sharing.spark.model.DeltaTableFiles;
import io.delta.sharing.spark.model.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.Seq$;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;


public class FirstTest {
    public static void main(String[] args) throws IOException {
        DeltaSharingProfileProvider profileProvider = new DeltaSharingJSONProvider("{\n" +
                "  \"shareCredentialsVersion\": 1,\n" +
                "  \"endpoint\": \"https://sharing.delta.io/delta-sharing/\",\n" +
                "  \"bearerToken\": \"faaie590d541265bcab1f2de9813274bf233\"\n" +
                "}");
        DeltaSharingRestClient httpClient = new DeltaSharingRestClient(profileProvider, 120, 4, false);
        Seq<Table> tables = httpClient.listAllTables();
        List<Table> tableList = JavaConverters.seqAsJavaList(tables);
        for (Table table : tableList) {
            System.out.println(table.name());
        }
        @SuppressWarnings("unchecked") Seq<String> nil = (Seq<String>) Seq$.MODULE$.empty();
        DeltaTableFiles deltaTableFiles = httpClient.getFiles(tableList.get(0), nil, scala.None$.apply(0));
        List<AddFile> files = JavaConverters.seqAsJavaList(deltaTableFiles.files());
        DeltaSharingFileSystem fs = new DeltaSharingFileSystem();
        fs.setConf(new Configuration());
        for (AddFile file : files) {
            System.out.println(file);
            FSDataInputStream stream = fs.open(DeltaSharingFileSystem.createPath(URI.create(file.url()), file.size()), 1024);
            Files.write(Paths.get("/Users/milos.colic/IdeaProjects/DeltaSharingJavaConnector/target/" +  file.id() + ".parquet"), stream.readAllBytes());
        }
    }
}
