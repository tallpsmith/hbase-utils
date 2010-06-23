package com.custardsource.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

/**
 * <p>
 * Convenience Builder Pattern that allows fluid configuration and construction of a {@link Scan}
 * instance for a given {@link HTable} and creating the {@link ResultScanner} output for use.
 * </p>
 * <p>
 * Examples:
 * </p>
 * 
 * <pre>
 * ResultScanner resultScanner = new ResultScannerBuilder(table).withColumnFamilies(&quot;columnFamily1&quot;,
 *         &quot;columnFamily2&quot;, &quot;columnFamily3&quot;).withFilter(filter).startAt(startRowKey)
 *         .stopAt(stopRowKey).build();
 * </pre>
 * 
 * @author paulsmith
 */
public class ResultScannerBuilder {

    private final List<byte[]> familyNames = Lists.newArrayList();
    private final HTable table;

    private byte[] startRow = null;
    private byte[] stopRow = null;

    private Filter filter = null;

    public ResultScannerBuilder(HTable table) {
        this.table = table;
    }

    public ResultScannerBuilder withColumnFamilies(String... columnFamilies) {
        return withColumnFamilies(HBaseFunctions.toByteArrays(columnFamilies));
    }

    public ResultScannerBuilder withColumnFamilies(byte[]... columnFamilies) {
        familyNames.addAll(Arrays.asList(columnFamilies));
        return this;
    }

    public ResultScanner build() throws IOException {
        Scan scan = new Scan();
        for (byte[] family : familyNames) {
            scan.addFamily(family);
        }

        if (startRow != null) {
            scan.setStartRow(startRow);
        }
        if (stopRow != null) {
            scan.setStopRow(stopRow);
        }

        if (filter != null) {
            scan.setFilter(filter);
        }

        return table.getScanner(scan);
    }

    public ResultScannerBuilder startAt(Writable startRowKey) {
        return startAt(HBaseUtils.forWritable(startRowKey));
    }

    public ResultScannerBuilder startAt(byte[] startRow) {
        this.startRow = startRow;
        return this;
    }

    public ResultScannerBuilder stopAt(Writable stopRow) {
        return stopAt(HBaseUtils.forWritable(stopRow));
    }

    public ResultScannerBuilder stopAt(byte[] stopRow) {
        this.stopRow = stopRow;
        return this;
    }

    public ResultScannerBuilder withFilter(Filter filter) {
        this.filter = filter;
        return this;
    }

}
