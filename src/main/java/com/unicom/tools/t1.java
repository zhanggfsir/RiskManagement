package com.unicom.tools;

import com.unicom.utils.Encryption;

public class t1 {
    public static void main(String[] args) {
        System.out.println("");

        // 拼接得到哈希串
        String s="018ef624a1e72c393e8df54c078d2d461fcc43336f6ce4cde5a9874ae15af079";
        int strs=s.toUpperCase().hashCode() & 0x7FFF ;
        System.out.println((short)strs);  //21621
        //b. 然后将10进制21621得到16进制 得到：5475   --> \x54\x75,

// 拼接得到哈希串
        String s1="537326DF490A63D9371779C5D811C2F0";
        int strs1=s1.toUpperCase().hashCode() & 0x7FFF ;
        System.out.println((short)strs1);  //18195
        //b. 然后将10进制21621得到16进制 得到：4713   --> \47\x13,

//        \47\x13537326DF490A63D9371779C5D811C2F020200205
//                \x47\x13 537326DF490A63D9371779C5D811C2F0 20200205

        System.out.println();



        System.out.println(Encryption.md5("13277001337"));
//        13277001337
//
        String ss1="1E5DA3B6270CEED21E439E2361265D2C";
        int ss11=ss1.toUpperCase().hashCode() & 0x7FFF ;
        System.out.println((short)ss11);  //12270 2fee \x2f\xee1E5DA3B6270CEED21E439E2361265D2C20200205

        System.out.println("--------------------");
//        CB12135EDE81EDE8392E189870CF832B
        System.out.println(Encryption.md5("18607191553"));
//        18607191553

        String ss2="CB12135EDE81EDE8392E189870CF832B";
        int ss22=ss2.toUpperCase().hashCode() & 0x7FFF ;
        System.out.println((short)ss22);  //11652 2d84  \x2d\x84CB12135EDE81EDE8392E189870CF832B20200205

//  get 'p_yiqing_hb',"\x2f\xee1E5DA3B6270CEED21E439E2361265D2C20200205"
//  get 'p_yiqing_hb',"\x2d\x84CB12135EDE81EDE8392E189870CF832B20200205"
//
//  get 'p_yiqing_wh',"\x2f\xee1E5DA3B6270CEED21E439E2361265D2C20200205"
//  get 'p_yiqing_wh',"\x2d\x84CB12135EDE81EDE8392E189870CF832B20200205"


        System.out.println("%%%%%555");
        String ss3="283D9B372B119A0789B15C8D09A34BF6";
        int ss33=ss3.toUpperCase().hashCode() & 0x7FFF ;
        System.out.println((short)ss33);  // 6630  \x30\xfd262CA5841FCF3D2936646608F90AEE6120200209
        //  \x66\x30283D9B372B119A0789B15C8D09A34BF6


        ///、、有数啊，加密后是这个 283D9B372B119A0789B15C8D09A34BF6

        System.out.println("-----");
        // 18675577260  4B37A2051560B5BA85E19A3951A0A66D
        System.out.println(Encryption.md5("18675577260"));
        System.out.println("-----");
        String s4="4B37A2051560B5BA85E19A3951A0A66D";
        int s41=s4.toUpperCase().hashCode() & 0x7FFF ;
        System.out.println((short)s41);  // 12029  2efd  \x30\xfd262CA5841FCF3D2936646608F90AEE6120200209
        //  \x2e\xfd4B37A2051560B5BA85E19A3951A0A66D



        // 15601027998 34D320D4188E7DCB58C4F48988817D24
        System.out.println(Encryption.md5("15601027998"));
        System.out.println("-----==");

        // 15601027998 34D320D4188E7DCB58C4F48988817D24
        System.out.println(Encryption.md5("15100026700"));
        System.out.println("-----==");


        String s5="4E4C46A31DE704393442D3BDBA2C607D";
        int s45=s5.toUpperCase().hashCode() & 0x7FFF ;
        System.out.println((short)s45);  //  17386  43ea

        //  \x43\xea4E4C46A31DE704393442D3BDBA2C607D




        String path="/user/ubd_master/ubd_risk_serv.db/dm_d_cus_jingxun_internet_taxi/month_id=201912/day_id=02/prov_id=018/00000_0.gz";
//        String month_id=path.toString().split("=")[1].split("/")[0];
//        String day_id=path.toString().split("=")[2].split("/")[0];
        String prov=path.toString().split("=")[3].split("/")[0];
        System.out.println(prov);


    }
}
