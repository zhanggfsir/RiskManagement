package com.unicom.utils;


import org.apache.commons.lang.StringUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Encryption
{
    public static String sha256(String strText)
    {
        return sha(strText, "SHA-256");
    }

    public static String sha521(String strText)
    {
        return sha(strText, "SHA-512");
    }

    public static String md5(String strText)
    {
        return sha(strText, "MD5");
    }

    public static String sha1(String strText)
    {
        return sha(strText, "SHA-1");
    }

    public static String sha(String strText, String strType)
    {
        String strResult = null;

        if (StringUtils.isNotBlank(strText))
        {
            try
            {
                MessageDigest messageDigest = MessageDigest.getInstance(strType);
                messageDigest.update(strText.getBytes());
                byte[] byteBuffer = messageDigest.digest();
                StringBuffer strHexString = new StringBuffer();
                for (int i = 0; i < byteBuffer.length; i++)
                {
                    String hex = Integer.toHexString(0xFF & byteBuffer[i]);
                    if (hex.length() == 1)
                    {
                        strHexString.append('0');
                    }
                    strHexString.append(hex);
                }
                strResult = strHexString.toString().toUpperCase();
            }
            catch (NoSuchAlgorithmException e)
            {
                e.printStackTrace();
            }
        }
        return strResult;
    }

}