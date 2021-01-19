package com.unicom.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class CommonUtil {
    private static Logger logger = LoggerFactory.getLogger(CommonUtil.class);

    public  List<String> getFilePath(File srcFile) {
        try {
            List<String> listAll = new ArrayList();
            if (srcFile.isFile()) {
                listAll.add(srcFile.getCanonicalPath());
                return listAll;
            }
            if (srcFile.isDirectory()) {
                for (File fi : srcFile.listFiles()) {
                    listAll.addAll(getFilePath(fi));
                }
                return listAll;
            }
            return null;
        } catch (Exception ex) {
            logger.error("can't get the local files' list------>{}", srcFile);
            ex.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {
        String text="17610001153";
//        8F311CD2ED87C497EC97DA7324BCD43B1448917B0C36927F0D64B1BF69566C7A
//        262CA5841FCF3D2936646608F90AEE61
        System.out.println(Encryption.sha256(text));
        System.out.println(Encryption.md5(text));

        String textd="";
        System.out.println(Encryption.sha256(textd));
        System.out.println(Encryption.md5(textd));

        String textNull=null;
        System.out.println(Encryption.sha256(textNull));
        System.out.println(Encryption.md5(textNull));



    }
}
