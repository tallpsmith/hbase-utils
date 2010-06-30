package com.custardsource.hbase;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.verify;


public class PutBuilderTest extends TestCase {

    @Mock
    HTable hTable;

    private static final int firstRowKey = 1;
    private static final int secondRowKey = 2;
    private static final String columnA = "columnA";

    private static final String columnB = "columnB";
    private static final String columnC = "columnC";
    private static final String columnD = "columnD";
    private static final String valueA = "valueA";

    private static final String valueB = "valueB";
    private static final String valueC = "valueC";
    private static final String valueD = "valueD";
    private static final String FOO = "foo";
    private static final String EEK = "eek";


    @Override
    protected void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    public void testBasics() throws IOException {
        final int cellValue = 1;
        PutBuilder builder = new PutBuilder(hTable);

        ArgumentCaptor<List> listCaptor = ArgumentCaptor.forClass(List.class);

        builder.withRowKey(firstRowKey).withColumnFamily(FOO).put(columnA, cellValue).putAll();

        verify(hTable).put(listCaptor.capture());
        List<Put> puts = listCaptor.getValue();

        assertEquals(cellValue, puts.size());
        Put put = puts.get(0);
        assertColumnFamily(put, FOO);
        assertPutExistsFor(put, FOO, columnA, Bytes.toBytes(cellValue), String.valueOf(cellValue));

    }


    private void assertPutExistsFor(Put put, String columnFamily, String columnQualifier, byte[] valueBytes, String valueAsString) {
        final List<KeyValue> keyValueList = put.get(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));
        boolean located = false;
        for (KeyValue keyValue : keyValueList) {
            final byte[] putValue = keyValue.getValue();
            if (Bytes.equals(putValue, valueBytes)) {
                located = true;
                break;
            }
        }
        assertTrue(String.format("Should have located a Put %s:%s with value %s", columnFamily, columnQualifier, valueAsString), located);
    }

    private void assertColumnFamily(Put put, String columnFamily) {
        assertEquals(columnFamily, Bytes.toString(put.getFamilyMap().keySet().iterator().next()));
    }

    public void testThatStringPutWorks() throws IOException {


        PutBuilder builder = new PutBuilder(hTable);
        // first Row

        builder.withRowKey(firstRowKey).withColumnFamily(FOO)
                .put(columnA, valueA)
                .put(columnB, valueB);
        // secondRow
        builder.withRowKey(secondRowKey).withColumnFamily(EEK)
                .put(columnC, valueC)
                .put(columnD, valueD);
        builder.putAll();

        ArgumentCaptor<List> listCaptor = ArgumentCaptor.forClass(List.class);


        verify(hTable).put(listCaptor.capture());
        List<Put> puts = listCaptor.getValue();
        assertEquals(4, puts.size());

        assertExpectedPutColumnQualifiers(puts, columnA, columnB, columnC, columnD);
        assertExpectedColumnFamiliers(puts, FOO, EEK);

    }


    private void assertExpectedColumnFamiliers(List<Put> puts, String... columnFamilies) {

        Function<Put, Collection<String>> putToColumnQualifiers = new Function<Put, Collection<String>>() {

            @Override
            public Collection<String> apply(Put put) {

                final Map<byte[], List<KeyValue>> familyMap = put.getFamilyMap();

                return Collections2.transform(familyMap.keySet(), HBaseFunctions.BYTES_TO_STRING);
            }
        };

        assertPutBuilderCondition(puts, putToColumnQualifiers, columnFamilies, "columnFamilies");

    }

    private void assertExpectedPutColumnQualifiers(List<Put> puts, String... keyNames) {

        Function<Put, Collection<String>> putToColumnQualifiers = new Function<Put, Collection<String>>() {

            @Override
            public Collection<String> apply(Put put) {

                Collection<String> columnQualifiers = Lists.newArrayList();

                final Map<byte[], List<KeyValue>> familyMap = put.getFamilyMap();

                for (List<KeyValue> keyValue : familyMap.values()) {
                    // TODO this actually means that perhaps if the rowKey hasn't changed, we should store the original put and use Put.add(..) ?
                    assertEquals(1, keyValue.size());
                    columnQualifiers.add(Bytes.toString(keyValue.get(0).getQualifier()));
                }

                return columnQualifiers;
            }
        };


        assertPutBuilderCondition(puts, putToColumnQualifiers, keyNames, "columnQualifiers");
    }

    private void assertPutBuilderCondition(List<Put> puts, Function<Put, Collection<String>> putToColumnQualifier, String[] keyNames, String inspectionName) {
        Set<String> keySet = Sets.newHashSet(keyNames);


        Set<String> putValue = Sets.newHashSet();

        for (Put put : puts) {
            putValue.addAll(putToColumnQualifier.apply(put));
        }

        Sets.SetView<String> setDifference = Sets.symmetricDifference(keySet, putValue);

        assertEquals(String.format("Should not have found any difference in expected %s but mismatches were: %s", inspectionName, setDifference), 0, setDifference.size());
    }
}
