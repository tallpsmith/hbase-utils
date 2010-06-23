package com.custardsource.hbase;

import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

/**
 * <p>
 * Provides a convenient transformation pattern to convert common cases from {@link Result} byte[],
 * by providing transformation {@link Function}s.
 * </p>
 * <p>
 * A {@link Result} necessarily deals with byte[] for the Column Qualifier and cell values, but in
 * many cases, there are common patterns of using Strings for Column Qualifies, and fairly often
 * even the Cell values.
 * </p>
 * <p>
 * Examples:
 * </p>
 * 
 * <pre>
 * NavigableMap&lt;String, String&gt; columnFamilyValues = ResultTransformer.forStrings(result,
 *         &quot;myColumnFamily&quot;).transform();
 * 
 * </pre>
 * 
 * @author paulsmith
 */
public class ResultTransformer<KEY extends Comparable<?>, VALUE> {

    private final NavigableMap<KEY, VALUE> transformedMap = Maps.newTreeMap();

    public ResultTransformer(Result result, String columnFamily,
            Function<byte[], KEY> keyTransformer, Function<byte[], VALUE> valueTransformer) {

        Map<byte[], VALUE> newMap = Maps.transformValues(result.getFamilyMap(Bytes
                .toBytes(columnFamily)), valueTransformer);

        for (Map.Entry<byte[], VALUE> entry : newMap.entrySet()) {
            transformedMap.put(keyTransformer.apply(entry.getKey()), entry.getValue());
        }

    }


    public static ResultTransformer<String, String> forStrings(Result result, String columnFamily) {

        return new ResultTransformer<String, String>(result, columnFamily,
                HBaseFunctions.BYTES_TO_STRING, HBaseFunctions.BYTES_TO_STRING);
    }

    public Map<KEY, VALUE> columnsStartingWith(final String prefix) {

        return Maps.filterKeys(transformedMap, new Predicate<KEY>() {

            @Override
            public boolean apply(KEY key) {
                return key.toString().startsWith(prefix);
            }
        });
    }

    public NavigableMap<KEY, VALUE> transform() {
        return transformedMap;
    }

}
