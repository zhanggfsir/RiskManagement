package com.unicom.queryImpl;

import com.unicom.utils.Encryption;

public class Md5Sha256 {
    public static void main(String[] args) {
        String number="18511967902";
        String md5= Encryption.md5(number);
        String sha256=Encryption.sha256(number);
        System.out.println(md5);
        System.out.println(sha256);

        //
//        262CA5841FCF3D29366466089F0AEE61
//        8F311CD2ED87C497EC97DA7324BCD43B1448917B0C36927F0D64B1BF69566C7A


        String path="hdfs://beh/user/lf_by_pro/zba_dwa.db/dwa_v_d_cus_al_user_info/prov_id_part=097/000000_0";
        String provId=path.toString().split("=")[1].split("/")[0];
        System.out.println(provId);

    }
}
