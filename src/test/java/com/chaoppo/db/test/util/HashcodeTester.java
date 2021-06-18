package com.chaoppo.db.test.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class HashcodeTester {

    /**
     * Whenever it is invoked on the same object more than once during an execution of a Java application, the hashCode
     * method must consistently return the same integer
     */
    public static void testHashCodeConsistency(Object o1) {
        assertEquals("object hashcode consistency with itself failed! Weird. ", o1.hashCode(), o1.hashCode());
    }

    /**
     * if o1 is equals to o2, then o1.hashcode should be equals to o2.hashcode
     *
     * @param o1
     * @param o2
     */
    public static void testHashCodeEquality(Object o1, Object o2) {
        if (o1.equals(o2)) {
            assertEquals("if o1 and o2 are equals, then they should have the same hashcode!", o1.hashCode(),
                    o2.hashCode());
        } else {
            assertNotEquals("if o1 and o2 are not equals, then they should not have the same hashcode!", o1.hashCode(),
                    o2.hashCode());
        }
    }
}
