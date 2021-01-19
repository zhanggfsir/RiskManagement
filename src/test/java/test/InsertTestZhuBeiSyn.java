package test;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.service.GetConfigInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

/*
测试样例

//        String tableName="user_daily_msisdn_test";
//        String columnName="ui";
//        String account="20190804";
//        String fileName="ci_dwa_v_d_cus_al_user_info";
//        // path 为了获得 省ID ，比如 011。为了方便直接使用实现类
//        String path="/user/lf_by_pro/zba_dwa.db/dwa_v_d_cus_al_user_info/part_id=201908/day_id=06/prov_id=011/000025_0";

 */
public class InsertTestZhuBeiSyn { //implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTestZhuBeiSyn.class);
    private static final Object lock = new Object();

    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException, IOException {

        String tableName=null;
        String columnName=null;
        String account=null;
        String fileName=null;
        String path=null;

        if(args.length==5){
            tableName=args[0];
            columnName=args[1];
            account=args[2];
            fileName=args[3];
            path=args[4];
        }else{
            logger.error("参数个数只能是5个：tableName columnName account fileName path");
            System.exit(-1);
        }


        String prefixPath="/root/zgf/data/";
        String suffixPath=".txt";
        String implMethod="getPut";

        //获的loadColumnInfo
        GetConfigInfo getConfigInfo=new GetConfigInfo();
        LoadColumnInfo loadColumnInfo=getConfigInfo.getLoadColumnInfo(tableName,columnName);

        // 读文件
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(prefixPath+fileName+suffixPath),"UTF-8"));

         //获得 加载方法对象 instance
        Class<?> implClass = getImplClass(loadColumnInfo);
        Object instance = implClass.newInstance();;

        //获得 table
        Table table = getTable(tableName);
        if (table == null) {
            System.exit(1);
        }

        Put put=null;
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            logger.info("读取字段的内容为 ----->"+line);
            logger.info("account ----->"+account);
            put = (Put) implClass.getDeclaredMethod(implMethod, String.class, LoadColumnInfo.class, Path.class,String.class ).invoke(instance, line, loadColumnInfo, new Path(path), account);
            try {
                table.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static Table getTable(String tableName) {
        Configuration configuration = HBaseConfiguration.create();
        // 172.16.1.128:2181
        //172.16.21.114:2181,172.16.21.115:2181,172.16.21.116:2181
        configuration.set("hbase.zookeeper.quorum","172.16.21.114:2181,172.16.21.115:2181,172.16.21.116:2181");
        configuration.set("zookeeper.znode.parent", "/hbase");

        Connection connection = null;
        Table table = null;
        synchronized (lock) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
                table = connection.getTable(TableName.valueOf(tableName));
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            lock.notifyAll();
        }
        return table;
    }


    public static Class<?>  getImplClass(LoadColumnInfo loadColumnInfo) throws ClassNotFoundException {
        File jarFile = new File(loadColumnInfo.getJarName().trim());
        // 从URLClassLoader类中获取类所在文件夹的方法，jar也可以认为是一个文件夹
        Method method = null;
        try {
            method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        } catch (NoSuchMethodException | SecurityException e1) {
            e1.printStackTrace();
        }
        // 获取方法的访问权限以便写回
        boolean accessible = method.isAccessible();
        try {
            method.setAccessible(true);
            // 获取系统类加载器
            URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
            URL url = jarFile.toURI().toURL();
            method.invoke(classLoader, url);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            method.setAccessible(accessible);
        }

        //"com.unicom.impl.individualizationImpl.PJingxunInternetTaxi"
        Class<?> implClass = null;
        implClass = Class.forName(loadColumnInfo.getClassName().trim());
        return implClass;

    }


    /*
    public static Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.AlUserInfo.Builder alUserInfoBuild= Risk.AlUserInfo.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }if(account.length()!=8){
            logger.error("输入账期的格式为 yyyyMMdd,当前输入账期为{},不符合规范", account);
            return null;
        }

        String provId=path.toString().split("=")[3].split("/")[0];
        alUserInfoBuild.setAreaId(array[1]);
        alUserInfoBuild.setUserId(array[2]);
        alUserInfoBuild.setServiceType(array[4]);
        alUserInfoBuild.setPayMode(array[5]);
        alUserInfoBuild.setProductId(array[6]);
        alUserInfoBuild.setProductMode(array[7]);
        alUserInfoBuild.setInnetDate(array[8]);

        try{
            int innetMonths = Integer.parseInt(array[9]);
            alUserInfoBuild.setInnetMonths(innetMonths);
        } catch (NumberFormatException e) {
        }
        try{
            int isCard = Integer.parseInt(array[10]);
            alUserInfoBuild.setIsInnet(isCard);
        } catch (NumberFormatException e) {
        }
        try{
            int isInnet = Integer.parseInt(array[11]);
            alUserInfoBuild.setIsInnet(isInnet);
        } catch (NumberFormatException e) {
        }
        try{
            int isThisAcct= Integer.parseInt(array[12]);
            alUserInfoBuild.setIsInnet(isThisAcct);
        } catch (NumberFormatException e) {
        }
        try{
            int isThisBreak= Integer.parseInt(array[13]);
            alUserInfoBuild.setIsInnet(isThisBreak);
        } catch (NumberFormatException e) {
        }
        alUserInfoBuild.setCloseDate(array[14]);
        alUserInfoBuild.setCustId(array[15]);
        alUserInfoBuild.setUserIdEn(array[16]);

        alUserInfoBuild.setUserStatus(array[20]);
        try{
            int isStat= Integer.parseInt(array[21]);
            alUserInfoBuild.setIsInnet(isStat);
        } catch (NumberFormatException e) {
        }
        alUserInfoBuild.setChannelId(array[24]);
        alUserInfoBuild.setStopType(array[25]);
        alUserInfoBuild.setProvId(provId);

        buff = alUserInfoBuild.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();

        String deviceNumberMd5=array[17].toUpperCase();
        keyValue.append(deviceNumberMd5);
        keyValue.append(account);
        //手机号哈希，手机号和账期作为key
        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
*/
}
