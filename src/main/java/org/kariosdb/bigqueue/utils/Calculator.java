package org.kariosdb.bigqueue.utils;

public class Calculator {
	
	
	/**
	 * mod by shift
	 * 
	 * @param val value to mod
	 * @param bits number of bits to shift
	 * @return answer
	 */
	public static long mod(long val, int bits) {
		return val - ((val >> bits) << bits);
	}
	
	/**
	 * multiply by shift
	 * 
	 * @param val value to multiply
	 * @param bits number of bits to shift
	 * @return answer
	 */
	public static long mul(long val, int bits) {
		return val << bits;
	}
	
	/**
	 * divide by shift
	 * 
	 * @param val value to divide
	 * @param bits number of bits to shift
	 * @return answer
	 */
	public static long div(long val, int bits) {
		return val >> bits;
	}

}
