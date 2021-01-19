package com.unicom.tools;

import com.unicom.utils.Encryption;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class tmp {
    public static void main(String[] args) {
        String s="2a";
        switch (s){
            case "1":
                System.out.println("1");
                break;
            case "a":
                System.out.println("a");
                break;
                default:
                System.out.println("default");
                break;
        }


        System.out.println(0x7FFF); //32767
        System.out.println(0x7FFF/100); //327
        System.out.println(0x7FFF-0x7FFF/100); //32440
        System.out.println("zgf".hashCode()); //120537cccc

//        admin.createTable(hTableDescriptor,
//                Bytes.toBytes((short) (0x7FFF / createTableInfo.getRegionNum())),
//                Bytes.toBytes((short) (0x7FFF - (0x7FFF / createTableInfo.getRegionNum()))),
//                createTableInfo.getRegionNum());

        System.out.println("-------------");
        byte[] temp = new byte[0];
         temp = Bytes.toBytes((short) ("1761000".hashCode() & 0x7FFF));
        System.out.println(temp);
        temp = Bytes.add(temp, Bytes.toBytes("176100_201912".toString()));
        System.out.println(temp);

        // 拼接得到哈希串
        String sa="018ef624a1e72c393e8df54c078d2d461fcc43336f6ce4cde5a9874ae15af079";
        System.out.println(sa.toUpperCase());
        int strs=sa.toUpperCase().hashCode() & 0x7FFF ;
        System.out.println((short)strs);  //21621

        System.out.println(Bytes.toBytes(strs));


        String path="/user/ubd_master/ubd_risk_serv.db/dm_d_cus_jingxun_internet_taxi/month_id=201912/day_id=02/00000_0.gz";
        String month_id=path.toString().split("=")[1].split("/")[0];
        String day_id=path.toString().split("=")[2].split("/")[0];

        System.out.println(month_id);
        System.out.println(day_id);

        System.out.println("-----------");
        String path1="/user/ubd_master/ubd_risk_serv.db/dm_d_cus_jingxun_internet_taxi/month_id=201912/day_id=02/prov_id=011/00000_0.gz";
        String month=path1.split("=")[1].split("/")[0];
        System.out.println(month);
        String day=path1.split("=")[2].split("/")[0];
        System.out.println(day);
        String prov=path1.split("=")[3].split("/")[0];
        System.out.println(prov);

        Encryption encryption=new Encryption();
        //262CA5841FCF3D2936646608F90AEE61
        System.out.println(encryption.md5("17610001153"));


        System.out.println("********");
        //262CA5841FCF3D2936646608F90AEE61
        System.out.println(new Encryption().md5("13247217666"));
        System.out.println(new Encryption().md5("15623728812"));
        System.out.println(new Encryption().md5("18627829199"));
        System.out.println(new Encryption().md5("15607166771"));
        System.out.println(new Encryption().md5("15611535334"));

        /*
        5ED7667BAE9F26E59B9D26A3C379013E
        17CD017B26ADE706196D7032582E6BA4
        5C5B9A24B4EF99C6D32E8F315740265D
        3243553BADC10B980C9CA1F4D1CC0ED5
        4E4F15C6F18442BD68795AFBFAD43FA5
            */

        System.out.println(new Encryption().md5("18610446278"));

//        Encryption encryption=new Encryption();
//        System.out.println(encryption.sha256("17610001153"));
//
//        String path2="/user/ubd_master/ubd_risk_serv.db/dm_d_cus_jingxun_internet_taxi/month_id=201912/prov=088/00000_0.gz";
//        String provId=path2.toString().split("=")[2].split("/")[0];
//        System.out.println(provId);
//
//
//        String path3="/user/ubd_master/ubd_risk_serv.db/dm_d_cus_jingxun_internet_taxi/month_id=201912/00000_0.gz";
//        String month3=path3.toString().split("=")[1].split("/")[0];
//        System.out.println(month3);


        System.out.println("0000000000000");
        System.out.println(new Encryption().md5("18565854707"));

        System.out.println("0000000000000"); // 262CA5841FCF3D2936646608F90AEE61
        System.out.println(new Encryption().md5("17610001153"));


        String a="xx";
        if(a.length()==11){
            System.out.println(11);
        }else if(a.length()==32){
            System.out.println(32);
        }else{
            System.out.println("unsupport");
        }

        System.out.println("0000000000000"); // B3340B45D02874E9356AFC8D5C809737
        System.out.println(new Encryption().md5("13035716260"));

//        18675577260
        // 4B37A2051560B5BA85E19A3951A0A66D
        System.out.println("0000000000000"); // B3340B45D02874E9356AFC8D5C809737
        System.out.println(new Encryption().md5("18675577260"));

        // 18623764186 266FDBC754100B08F2EF6CB691FA6C45
        System.out.println("0000000000000"); // 266FDBC754100B08F2EF6CB691FA6C45
        System.out.println(new Encryption().md5("18623764186"));


//        18675577260
        // 18675577260 4B37A2051560B5BA85E19A3951A0A66D
        System.out.println("0000000000000"); // 4B37A2051560B5BA85E19A3951A0A66D
        System.out.println(new Encryption().md5("18675577260"));


//        13134745208 04045C04845B422E4D73D3DC6860753E
        System.out.println("0000000000000"); //
        System.out.println(new Encryption().md5("13134745208"));



        //   江苏  13063789248 51AE19AEB0FD0006F4DB6CFF75D8B57A
        System.out.println("0000000000000"); //
        System.out.println(new Encryption().md5("13063789248"));

        //   深圳  18576693681  4087E71009D5D81F99A6138A66CACC03
        System.out.println("0000000000000"); //
        System.out.println(new Encryption().md5("18576693681"));



//        13067997753@张广峰@技术部 该客户一直在南京 短信验证结果为南京 请帮忙处理一下
//
//        18682459227 还有这个 一直在深圳 显示到过河南商丘

        System.out.println("------------");
        // 13067997753 18BF4A2B7497AC13D7B1C9D0F34175CD
        System.out.println(new Encryption().md5("13067997753"));

        //   深圳  18682459227 1DB40E2299C5B5AD278EBEABA7E7B7DE
        System.out.println(new Encryption().md5("18682459227"));

        List<String> ls=new ArrayList<>() ;
        StringBuffer sb=new StringBuffer();
        ls.add("a");
        ls.add("b");
        ls.add("c");
        System.out.println(ls.get(0));
        sb.append(ls.get(0));
        if(ls.size()>1){
            int len=ls.size();
            for (int i=1;i<=len-1;i++){
                sb.append("#");
                sb.append(ls.get(i));
            }
        }
        System.out.println(sb.toString());


//        13066890866
        //   剔除 湖北 13066890866 63FA0C30D9579F35F98D37D9669BB7D4
        System.out.println(new Encryption().md5("13066890866"));


        //        13155590612 湖北黄冈
        //   剔除 湖北 13155590612 5D054E641EE2611311105B9A39E20762
        System.out.println(new Encryption().md5("13155590612"));





        //        13063915052 一直在浙江
        //   剔除 湖北 13063915052  2A3EA75E0FC5AE59544D0484B5B3A3BD
        System.out.println(new Encryption().md5("13063915052"));


        //        18620539929
        //   18620539929  C8567DB2859F97CA656BE1E7019FEE91
        System.out.println(new Encryption().md5("18620539929"));



        //        18646754676
        //   18646754676  16E385CE3AC4F5EE3BD3C6DB44403199
        System.out.println(new Encryption().md5("18646754676"));


        System.out.println("---------");

        // 2       18568678765 18568678765
        //   18568678765  D94163C37382B2841750114662D18A1E
        System.out.println(new Encryption().md5("18568678765"));


        // 3       18505598789
        //   18505598789   9FCA62D5F602847F8654CEA76C4AEC87
        System.out.println(new Encryption().md5("18505598789"));


        // 3       18505124787
        //   18505124787  104B48F0DF6D09EB5A59F636E8FA1414
        System.out.println(new Encryption().md5("18505124787"));


        System.out.println("----------------");
        // 18607911200  D44DB271B4B4CE1CD550545DC015B717 浙江反馈，这两个号码是联通的同事，底层没有查询到号码的位置信息。

        System.out.println(new Encryption().md5("18607911200"));

        // 18679100266   B8780809071BC36B6C21E5CFB7B2A913 浙江反馈，这两个号码是联通的同事，底层没有查询到号码的位置信息。
        System.out.println(new Encryption().md5("18679100266"));

        // 18657137772   9592D16EDC36C59D4F74B2C8C55A5055 客户反馈没有到过江西
        System.out.println(new Encryption().md5("18657137772"));

        // 13003246713  0A72D6D8DC991E71B0CD3DAE183A7AC8 还有这个 反馈没有出过上海
        System.out.println(new Encryption().md5("13003246713"));


        // 13267256693 实际到达永州市，东莞市  9EC1A17041947A95E1603B084151F55A
        System.out.println(new Encryption().md5("13267256693"));


        // 16628568128  7AD7502999209805266A3D9886E6CA49
        System.out.println(new Encryption().md5("16628568128"));

        // 18501397082 108E3E22CD6B0885F67DC3ECD0DA8041
        System.out.println(new Encryption().md5("18501397082"));


//        号码：15551062598 624B8849EE4C0DA4E6CF897CEF04F2B7
//        描述：
//        客户目前在，安徽
//        查询咱们系统，去过江苏徐州，江苏南京，泰州市，盐城市；安徽省马鞍山市；
//        通过10010短信查询只到访过安徽马鞍山市，江苏南京市；
//        客户希望咱们优化数据
        System.out.println(new Encryption().md5("15551062598"));


//        13056907077           D7D328DF1C7144B4C3DD854D7755953E
        System.out.println(new Encryption().md5("13056907077"));


        System.out.println("*********");
        //  15651160293 9F296A4E1DBAC4B1643290F334A60526
        System.out.println(new Encryption().md5("15651160293"));
        // 15651600771 D8982AFF40F92C3539304786F0FA7659
        System.out.println(new Encryption().md5("15651600771"));

// 13071911229      2CCF8CA7490A74C90EADB949A302E3D4
        System.out.println(new Encryption().md5("13071911229"));



        // 13178001328  C39018DC4A889B798298C7DA4A7C318B
 //  13178001328 福建省泉州市用户，16号可以查询到，但现在查询不到。
        System.out.println(new Encryption().md5("13178001328"));


        // 13291176688  AAD0E8B262B5F05B6CC042FC81AAB18F
//        查询咱们系统，去过河南省安阳市，信阳市，商丘市；
//        通过10010短信查询只到访过河南省商丘市；
        System.out.println(new Encryption().md5("13291176688"));

//        18550487060   36BC07BB5150F7E9BB5C78011D02D786
//        查询咱们系统，去过江苏省苏州市，上海市；
//        通过10010短信查询只到访过江苏省苏州市
        System.out.println(new Encryption().md5("18550487060"));


//        13203899005

//        13203899005 实际在郑州，扫码还有周口。
     // 13203899005 实际在郑州，扫码还有周口。 CBA2FB08CF159800AD2AB67D6504DB9C
        System.out.println(new Encryption().md5("13203899005"));



        // 13035716260  B3340B45D02874E9356AFC8D5C809737
        System.out.println(new Encryption().md5("13035716260"));

        // 1FD6C41DB4F974FB046C7C4237F60F7A
//        13140025849 实际在郑州，扫码还有周口。短信是对的
        System.out.println(new Encryption().md5("13140025849"));

//        18579082982这个扫码查不出 A5FC6F454DED68929F27ABFA5C309038
        System.out.println(new Encryption().md5("18579082982"));

//        17621309087这个2月1日开始一直在上海，扫码不准确，请更正 AE34080E1725061FEAF59BF3128A15A6
        System.out.println(new Encryption().md5("17621309087"));

//        18624886064  这个一直在安阳 528D67C829935659531539664C3824D5
        System.out.println(new Encryption().md5("18624886064"));

//        13035716260
        //        13035716260  通过数据中心接口查询，客户只出现在广东河源市 B3340B45D02874E9356AFC8D5C809737
        System.out.println(new Encryption().md5("13035716260"));


//        通过数据中心接口查询，客户只出现在江苏省南通市

        //        13088865631
        //        13088865631  通过数据中心接口查询，客户只出现在江苏省南通市，客户只出现在广东河源市 249A27C4F52950D622539CDAE1B6C4CC
        System.out.println(new Encryption().md5("13088865631"));

    }

}

