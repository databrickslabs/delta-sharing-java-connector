==================
TableReader
==================

DeltaSharing instance (sharing in code) can be used to obtain a TableReader instance that allows you to consume tables in your code.
TableReader provides read() - reads 1 row, readN(n) - reads n rows, and readAll() - reads all rows, but should be avoided since it reads all the data in one go.
We advise that you use read() or readN() to iteratively consume your data. read() and readN() will read from available input streams
in a queue fashion until all streams are completely consumed. Each part file from the table will have a separate stream managed by the TableReader.
TableReader will have a fixed view of the table for each instance. To refetch data from the remote server you will need to generate a new TableReader.

Examples
************

.. tabs::
   .. code-tab:: java

    import com.databricks.labs.delta.sharing.java.format.parquet.TableReader;
    import org.apache.avro.generic.GenericRecord;

    TableReader<GenericRecord> tableReader = sharing.getTableReader(“table.coordinates”);

    tableReader.read(); //returns 1 row
    tableReader.readN(20); //returns next 20 rows



   .. code-tab:: scala

    val tableReader = sharing
      .getTableReader(“table.coordinates”)

    tableReader.read() //returns 1 row
    tableReader.readN(20) //returns next 20 rows
