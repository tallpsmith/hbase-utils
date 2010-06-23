package com.custardsource.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;

/**
 * <p>
 * General Utility functions for overarching common HBase functions.
 * </p>
 * <p>
 * While Hbase API needs to be able to declare IOExceptions in many cases, from a client perspective
 * there is generally little to do but propagate this as a RuntimeException up to a top-level
 * handler, so this class facilitates this as a common pattern, generally wrapping any Exception in
 * a {@link RuntimeException}.
 * </p>
 * 
 * @author paulsmith
 */
public class HBaseUtils {

    /**
     * The {@link HTable} is closed, any Exception caught during close is swallowed.
     * 
     * @param table
     */
    public static void closeQuietly(HTable table) {
        try {
            table.close();
        } catch (Exception e) {
        }
    }

    /**
     * Attempt to flushCommits, any {@link IOException} caught is propagated as a
     * {@link RuntimeException}
     * 
     * @param table
     */
    public static void flushQuietly(HTable table) {
        try {
            table.flushCommits();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decodes a {@link Writable} into a byte[], propagating any {@link IOException} as a
     * {@link RuntimeException}
     * 
     * @param writable
     * @return
     */
    public static byte[] forWritable(Writable writable) {
        try {
            return Writables.getBytes(writable);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
