package com.chaoppo.db.test.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class EqualsTester {

    /**
     * according to Object.equals for any non-null reference value x, x.equals(x) should return true.
     *
     * @param o
     */
    public static void testReflexive(Object o) {
        assertEquals("Object o should be reflexibly equal to itself.", o, o);
    }

    /**
     * for any non-null reference values x and y, x.equals(y) should return true if and only if y.equals(x) returns
     * true.
     *
     * @param o1
     * @param o2
     */
    public static void testSymmetric(Object o1, Object o2) {
        assertEquals("o1 and o2 should be symetrically equal to each other.", o1, o2);

        assertEquals("o2 and o1 should be symetrically equal to each other.", o2, o1);
    }

    /**
     * for any non-null reference values x, y, and z, if x.equals(y) returns true and y.equals(z) returns true, then
     * x.equals(z) should return true.
     *
     * @param o1
     * @param o2
     * @param o3
     */
    public static void testTransitive(Object o1, Object o2, Object o3) {
        assertEquals("o1 should transitively be equal to o2.", o1, o2);

        assertEquals("o2 should transitively be equal to o3.", o2, o3);

        assertEquals("o1 should transitively be equal to o3.", o1, o3);
    }

    /**
     * For any non-null reference value x, x.equals(null) should return false.
     *
     * @param o1
     */
    public static void testNonNullity(Object o1) {
        assertFalse("o1 should not be equals to null!", o1.equals(null));
    }

    /**
     * test that o1 and o2 are different
     *
     * @param o1
     * @param o2
     */
    public static void testDifferent(Object o1, Object o2) {
        assertFalse("o1 should not be equals to o2.", o1.equals(o2));
    }

    public static void testDifferentObjectClass(Object o1, Object o2) {
        assertFalse("o1 should not be equals to o2.", o1.equals(o2));
    }
}
