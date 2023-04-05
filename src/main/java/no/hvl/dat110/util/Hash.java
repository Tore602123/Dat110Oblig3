package no.hvl.dat110.util;

/**
 * exercise/demo purpose in dat110
 * @author tdoy
 *
 */

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash { 
	
	
	public static BigInteger hashOf(String entity) {	
		
		BigInteger hashint = null;
		
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(entity.getBytes());
			byte[] digest = md.digest();
			String hexformat = toHex(digest);
			hashint = new BigInteger(hexformat, 16);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return hashint;
	}
	
	public static BigInteger addressSize() {
		

		int digestLength = 0;
		try {
			 digestLength = MessageDigest.getInstance("MD5").getDigestLength(); 

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		
		BigInteger StorInteger = new BigInteger("2");
		int BigBits = digestLength * 8;

		
		return StorInteger.pow(BigBits);
	}
	
	public static int bitSize() {
		
		int digestlen = 0;
		
		try {
			digestlen = MessageDigest.getInstance("MD5").getDigestLength();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		
		return digestlen*8;
	}
	
	public static String toHex(byte[] digest) {
		StringBuilder strbuilder = new StringBuilder();
		for(byte b : digest) {
			strbuilder.append(String.format("%02x", b&0xff));
		}
		return strbuilder.toString();
	}

}
