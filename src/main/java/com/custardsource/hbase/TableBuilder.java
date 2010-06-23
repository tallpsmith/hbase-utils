package com.custardsource.hbase;

import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <p>
 * Convenience Builder pattern class to quickly create a {@link HTable} class based on
 * common-patterns in a fluid motion.
 * </p>
 * <p>
 * Examples:
 * </p>
 * 
 * <pre>
 * HTable table = new TableBuilder(hbaseConfiguration).withTableName(&quot;tableName&quot;)
 *         .withSimpleColumnFamilies(&quot;columnFamily1&quot;, &quot;columnFamily2&quot;, &quot;columnFamily3&quot;)
 *         .deleteAndRecreate();
 * </pre>
 * 
 * @author psmith
 */
public class TableBuilder {

    private String tableName;
    private int maxVersions = 1;
    private final HBaseConfiguration configuration;

    private final List<HColumnDescriptor> columnFamilyDescriptors = Lists.newArrayList();

    public TableBuilder(HBaseConfiguration configuration) {
        Preconditions.checkNotNull(configuration);
        this.configuration = configuration;
    }

    public TableBuilder withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * Configures any Column Family created after this call to utilise this MaxVersion setting.
     * 
     * @see HColumnDescriptor#setMaxVersions(int)
     * @param maxVersions
     * @return
     */
    public TableBuilder withMaxVersions(int maxVersions) {
        this.maxVersions = maxVersions;
        return this;
    }

    /**
     * If the named Table does not exist within this {@link HBaseConfiguration}, it is created,
     * otherwise the existing table is disabled, deleted, and then created
     * 
     * @return
     * @throws Exception
     */
    public HTable deleteAndRecreate() throws Exception {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        if (hBaseAdmin.tableExists(tableName)) {
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }
        return create();
    }

    /**
     * Creates a set of Column Families using the current defined Column Family configuration.
     * 
     * @see #withMaxVersions(int)
     * @param familyNames
     * @return
     */
    public TableBuilder withSimpleColumnFamilies(String... familyNames) {
        for (String string : familyNames) {
            HColumnDescriptor columnFamilyDescriptor = new HColumnDescriptor(string);
            columnFamilyDescriptor.setMaxVersions(maxVersions);
            columnFamilyDescriptors.add(columnFamilyDescriptor);
        }
        return this;
    }

    /**
     * Causes all stored configuration to be applied and a {@link HTable} configured and returned
     * 
     * @return
     * @throws Exception
     */
    public HTable create() throws Exception {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        Preconditions.checkState(!hBaseAdmin.tableExists(tableName));

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        for (HColumnDescriptor descriptor : columnFamilyDescriptors) {
            tableDescriptor.addFamily(descriptor);
        }
        hBaseAdmin.createTable(tableDescriptor);
        return new HTable(configuration, tableName);

    }

}
