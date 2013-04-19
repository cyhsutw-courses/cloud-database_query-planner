package org.vanilladb.core.util;

/**
 * Provides utility methods for converting numeric values to/from bytes.
 */
public class ByteHelper {
	public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
	public static final int LONG_SIZE = Long.SIZE / Byte.SIZE;
	public static final int DOUBLE_SIZE = Double.SIZE / Byte.SIZE;

	public static byte[] toBytes(int val) {
		byte[] result = new byte[INT_SIZE];
		for (int i = 0; i < INT_SIZE; i++)
			result[i] = (byte) (val >> (INT_SIZE - 1 - i) * 8);
		return result;
	}

	public static byte[] toBytes(long val) {
		byte[] result = new byte[LONG_SIZE];
		for (int i = 0; i < LONG_SIZE; i++)
			result[i] = (byte) (val >> (LONG_SIZE - 1 - i) * 8);
		return result;
	}

	public static byte[] toBytes(double val) {
		return toBytes(Double.doubleToRawLongBits(val));
	}

	public static int toInteger(byte[] b) {
		int ret = 0;
		for (int i = 0; i < INT_SIZE; i++) {
			ret <<= 8;
			ret |= (int) b[i] & 0xFF;
		}
		return ret;
	}

	public static long toLong(byte[] b) {
		long ret = 0;
		for (int i = 0; i < LONG_SIZE; i++) {
			ret <<= 8;
			ret |= (long) b[i] & 0xFF;
		}
		return ret;
	}

	public static double toDouble(byte[] b) {
		long l = toLong(b);
		return Double.longBitsToDouble(l);
	}
}
