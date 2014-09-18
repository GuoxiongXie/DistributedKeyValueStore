package edu.berkeley.cs162;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

//import edu.berkeley.cs162.KVMessage;


public class bbc {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String vi;
		KVCrypt cry = new KVCrypt();
		String val = "abc";
		cry.setUp();
		cry.setCipher();
		System.out.printf("My string is %s\n", val);
		byte[] ena = cry.encrypt(KVMessage.encodeObject(val));
		String cctv = KVMessage.encodeObject(ena);
//		byte[] ena = cry.encrypt(val);
		
		
		
//		vi = new String(ena, "UTF8");
		String encrypt_value = (String) KVMessage.decodeObject(cry.decrypt((byte[]) KVMessage.decodeObject(cctv)));
		System.out.println("The final value should be (abc): " + encrypt_value);
//		System.out.printf("Encrypted string is %s\n", encrypt_value);
	}

}
