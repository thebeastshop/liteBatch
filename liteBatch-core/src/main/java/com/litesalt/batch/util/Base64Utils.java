package com.litesalt.batch.util;  

import java.io.IOException;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 * @author everywhere.z
 * @date 2017-3-1
 * @description Base64字符串与字节码转换工具
 */

@SuppressWarnings("restriction")
public class Base64Utils {  
    public static String encode(String str){
    	return new BASE64Encoder().encode(str.getBytes());
    }
    
    public static String decode(String str){
    	try {
			return new String(new BASE64Decoder().decodeBuffer(str));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
    }
    
    public static void main(String[] args) {
    	String encodeStr = Base64Utils.encode("i am a space hole");
		System.out.println(encodeStr);
		System.out.println(Base64Utils.decode(encodeStr));
	}
          
}  