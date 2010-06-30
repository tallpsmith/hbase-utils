package com.custardsource.hbase;

import junit.framework.TestCase;


public class HBaseFunctionsTest extends TestCase {


    public void testToBytesAndBackAgain() {

        /*
         * Yes this is probably just a re-test of Hadoop/HBase methods, but trying to be a bit TDD here. 
         */
        String test = "See you on the flip side";

        byte[] bytes = HBaseFunctions.STRING_TO_BYTES.apply(test);
        String roundTrip = HBaseFunctions.BYTES_TO_STRING.apply(bytes);

        assertEquals(test, roundTrip);
    }

}
