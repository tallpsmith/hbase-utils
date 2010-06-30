package com.custardsource.hbase;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Collection;


/**
 * Provides basic Google Guava/Collections Functions for transformations back and forth for common cases,
 * such as String->byte[].
 *  
 */
public class HBaseFunctions {

    public static final Function<String, byte[]> STRING_TO_BYTES = new Function<String, byte[]>() {
        @Override
        public byte[] apply(String columnFamily) {
            return Bytes.toBytes(columnFamily);
        }
    };

    public static final Function<byte[], String> BYTES_TO_STRING = new Function<byte[], String>() {

        @Override
        public String apply(byte[] b) {
            return Bytes.toString(b);
        }

    };

    public static byte[][] toByteArrays(String... strings) {
        Collection<byte[]> transform = Collections2.transform(Arrays.asList(strings),
                STRING_TO_BYTES);

        byte[][] array = transform.toArray(new byte[0][]);
        return array;
    }

}
