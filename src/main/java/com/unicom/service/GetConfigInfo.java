package com.unicom.service;

import com.unicom.entity.CreateTableInfo;
import com.unicom.entity.LoadColumnInfo;
import com.unicom.entity.LogAccoutInfo;
import com.unicom.entity.ZkInfo;
import com.unicom.tools.InsertTable;
import com.unicom.utils.JdbcUtil;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
    为了配合搭建bdi测试环境，如果是
 */
public class GetConfigInfo {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    /**
     * 获取zk的信息
     * @return
     */

    public ZkInfo getZkInfo(String zkName) {

        JdbcUtil jdbc=new JdbcUtil();
        Connection conn=jdbc.getConnection();
        System.out.println(zkName);
        String sql="select zk_id,zk_name,zk_quorum,zk_parent,zk_desc,mask from rm_config_zk where zk_name=?";
        PreparedStatement st= null;
        ResultSet rs;
        ZkInfo zkInfo = null;
        try {
            st = conn.prepareStatement(sql);
            st.setString(1,zkName);
            rs=st.executeQuery();

            if(rs.next()){
                zkInfo=new ZkInfo();
                zkInfo.setZkId(rs.getInt("zk_id"));
                zkInfo.setZkName(zkName);
                zkInfo.setZkQuorum(rs.getString("zk_quorum"));
                zkInfo.setZkParent(rs.getString("zk_parent"));
                zkInfo.setZkDesc(rs.getString("zk_desc"));
                zkInfo.setMask(rs.getString("mask"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            jdbc.close();
        }
        return zkInfo;
    }
    /**
     * 获取zk的信息
     * @return
     */

    public ZkInfo getZkInfo(String zkName,String tableName) {

        JdbcUtil jdbc=new JdbcUtil();
        Connection conn=jdbc.getConnection(tableName);
        System.out.println(zkName);
        String sql="select zk_id,zk_name,zk_quorum,zk_parent,zk_desc,mask from rm_config_zk where zk_name=?";
        PreparedStatement st= null;
        ResultSet rs;
        ZkInfo zkInfo = null;
        try {
            st = conn.prepareStatement(sql);
            st.setString(1,zkName);
            rs=st.executeQuery();

            if(rs.next()){
                zkInfo=new ZkInfo();
                zkInfo.setZkId(rs.getInt("zk_id"));
                zkInfo.setZkName(zkName);
                zkInfo.setZkQuorum(rs.getString("zk_quorum"));
                zkInfo.setZkParent(rs.getString("zk_parent"));
                zkInfo.setZkDesc(rs.getString("zk_desc"));
                zkInfo.setMask(rs.getString("mask"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            jdbc.close();
        }
        return zkInfo;
    }


    /**
     * 建表：从数据库获取建表信息
     * @param tableName
     * @return
     * @throws SQLException
     */
    public CreateTableInfo getConfigTableInfo(String tableName)  {
        JdbcUtil jdbc=new JdbcUtil();
        Connection conn=jdbc.getConnection(tableName);
        String sql="select table_id,table_name,table_type,interface_type,max_versions,min_versions,block_size,block_cache_enabled,in_memory,time_to_live,bloom_filter_type,compression_type,cache_blooms_on_write,cache_data_on_write,cache_indexes_on_write,compress_tags,scope,keep_deleted_cells,data_block_encoding,region_num,start_key,end_key,family_name,creator,table_desc,mask " +
                "from rm_config_table_info where table_name=?";
        PreparedStatement st= null;
        ResultSet rs=null;
        CreateTableInfo configTableInfo=null;
        try {
            st = conn.prepareStatement(sql);
            st.setString(1, tableName);
            rs=st.executeQuery();
            if (rs.next()){
                configTableInfo=new CreateTableInfo();
                configTableInfo.setTableId(rs.getInt("table_id"));
                configTableInfo.setTableName(rs.getString("table_name"));
                configTableInfo.setTableType(rs.getInt("table_type"));
                configTableInfo.setInterfaceType(rs.getInt("interface_type"));
                configTableInfo.setMaxVersions(rs.getInt("max_versions"));
                configTableInfo.setMinVersions(rs.getInt("min_versions"));
                configTableInfo.setBlockSize(rs.getInt("block_size"));
                configTableInfo.setBlockCacheEnabled(rs.getBoolean("block_cache_enabled"));
                configTableInfo.setInMemory(rs.getBoolean("in_memory"));
                configTableInfo.setTimeToLive(rs.getInt("time_to_live"));
                configTableInfo.setBloomFilterType(rs.getString("bloom_filter_type"));
                configTableInfo.setCompressionType((rs.getString("compression_type")));
                configTableInfo.setCacheBloomsOnWrite(rs.getBoolean("cache_blooms_on_write"));
                configTableInfo.setCacheDataOnWrite(rs.getBoolean("cache_data_on_write"));;
                configTableInfo.setCacheIndexesOnWrite(rs.getBoolean("cache_indexes_on_write"));
                configTableInfo.setCompressTags(rs.getBoolean("compress_tags"));
                configTableInfo.setScope(rs.getInt("scope"));
                configTableInfo.setKeepDeletedCells(rs.getBoolean("keep_deleted_cells"));
                configTableInfo.setDataBlockEncoding(rs.getString("data_block_encoding"));
                configTableInfo.setRegionNum(rs.getInt("region_num"));
                configTableInfo.setStartKey(rs.getInt("start_key"));
                configTableInfo.setEndKey(rs.getInt("end_key"));
                configTableInfo.setFamilyName(rs.getString("family_name"));
                configTableInfo.setCreator(rs.getString("creator"));
                configTableInfo.setTableDesc(rs.getString("table_desc"));
                configTableInfo.setMask(rs.getString("mask"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            jdbc.close();
        }
        return configTableInfo;
    }

    public LoadColumnInfo getLoadColumnInfo(String tableName,String columnName) {

        JdbcUtil jdbc=new JdbcUtil();
        Connection conn=jdbc.getConnection(tableName);

        String sql="select b.column_id,b.table_id,b.column_name,b.column_desc,b.is_hdfs,b.name_service,b.file_path,b.partitions,b.seperator,b.field_num,b.family_name,b.compress_type,b.thread_num,jar_name,class_name,query_class_name,b.creator,b.mask " +
                "from  (select table_name,table_id from rm_config_table_info where table_name=?) a  " +
                "inner join " +
                "(select column_id,table_id,column_name,column_desc,is_hdfs,name_service,file_path,partitions,seperator,field_num,family_name,compress_type,thread_num,jar_name,class_name,query_class_name,creator,mask from rm_config_load_column_info where column_name=?) b " +
                "on a.table_id=b.table_id;";

        logger.info(sql);
        PreparedStatement st= null;
        ResultSet rs=null;
        LoadColumnInfo loadColumnInfo=null;
        try {

            st = conn.prepareStatement(sql);
            st.setString(1, tableName);
            st.setString(2, columnName);

            logger.info(sql);
            logger.info(tableName);
            logger.info(columnName);

            rs=st.executeQuery();

            if (rs.next()){
                loadColumnInfo=new LoadColumnInfo();
                loadColumnInfo.setColumnId(rs.getInt("column_id"));
                loadColumnInfo.setTableId(rs.getInt("table_id"));
                loadColumnInfo.setColumnName(rs.getString("column_name"));
                loadColumnInfo.setColumnDesc(rs.getString("column_desc"));
                loadColumnInfo.setIsHdfs(rs.getBoolean("is_hdfs"));

                loadColumnInfo.setNameService(rs.getString("name_service"));
                loadColumnInfo.setFilePath(rs.getString("file_path"));
                loadColumnInfo.setPartitions(rs.getString("partitions"));
                loadColumnInfo.setSeperator(rs.getString("seperator"));
                loadColumnInfo.setFieldNum(rs.getInt("field_num"));
                loadColumnInfo.setFamilyName(rs.getString("family_name"));
                loadColumnInfo.setCompressType(rs.getString("compress_type"));

                loadColumnInfo.setThreadNum(rs.getInt("thread_num"));
                loadColumnInfo.setJarName(rs.getString("jar_name"));
                loadColumnInfo.setClassName(rs.getString("class_name"));
                loadColumnInfo.setQueryClassName(rs.getString("query_class_name"));
                loadColumnInfo.setCreator(rs.getString("creator"));
                loadColumnInfo.setMask(rs.getString("mask"));
                logger.info(loadColumnInfo.toString());
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            jdbc.close();
        }

        return loadColumnInfo;
    }


    //账期表 当没有数据时 插入；如果有更新
    public void addLogAccountInfo(LogAccoutInfo logAccoutInfo,String tableName) {
        JdbcUtil jdbc=new JdbcUtil();
        Connection conn=jdbc.getConnection(tableName);
        PreparedStatement st= null;
        int queryResult=0;

        //查询数据是否具备
        String querySql="select count(1) from rm_log_account_period where table_id=? and column_id=? ";
        try {
        st = conn.prepareStatement(querySql);
        st.setInt(1,logAccoutInfo.getTableId());
        st.setInt(2,logAccoutInfo.getColumn_id());

        ResultSet rs=st.executeQuery();
        queryResult=rs.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        String columnName="";
        // 分别获得 table_name column_name
        String tableNameSql="select table_name from rm_config_table_info where table_id=? ";
        try {
            st = conn.prepareStatement(tableNameSql);
            st.setInt(1,logAccoutInfo.getTableId());

            ResultSet rs=st.executeQuery();
            tableName=rs.getString(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String columnNameSql="select column_name from rm_config_load_column_info where  column_id=? ";
        try {
            st = conn.prepareStatement(columnNameSql);
            st.setInt(1,logAccoutInfo.getColumn_id());

            ResultSet rs=st.executeQuery();
            columnName=rs.getString(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }



        int rowNum=0;
        // 如果有数据 更新数据
        if(queryResult>0){
            String sql=" UPDATE rm_log_account_period SET account_period=? WHERE table_id=? and column_id=? ";
            try {
            st=conn.prepareStatement(sql);
            st.setString(1,logAccoutInfo.getAccountPeriod());
            st.setInt(2,logAccoutInfo.getTableId());
            st.setInt(3,logAccoutInfo.getColumn_id());
            rowNum=st.executeUpdate();
            if (rowNum>0){
                logger.info("更新日志表 rm_log_account_period 成功 ! "
                        +"表ID-->"+logAccoutInfo.getTableId()+" 表名："+tableName+" 列ID-->"+logAccoutInfo.getTableId()+"列名："+columnName);
            }else {
                logger.info("更新日志表 rm_log_account_period 失败 ! "
                        +"表ID-->"+logAccoutInfo.getTableId()+" 表名："+tableName+" 列ID-->"+logAccoutInfo.getTableId()+"列名："+columnName);
            }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //如果无数据 直接插入
        else{
            String sql="insert into rm_log_account_period values(?,?,?,?,null);";

            try {
                st = conn.prepareStatement(sql);
                st.setInt(1,logAccoutInfo.getTableId());
                st.setInt(2,logAccoutInfo.getColumn_id());
                st.setString(3,logAccoutInfo.getAccountPeriod());
                st.setString(4,logAccoutInfo.getCycle());
                rowNum=st.executeUpdate();

                if (rowNum>0){
                    logger.info("插入日志表 rm_log_account_period 成功 ! "
                            +"表ID-->"+logAccoutInfo.getTableId()+" 表名："+tableName+" 列ID-->"+logAccoutInfo.getTableId()+"列名："+columnName);
                }else {
                    logger.info("插入日志表 rm_log_account_period 失败 ! "
                            +"表ID-->"+logAccoutInfo.getTableId()+" 表名："+tableName+" 列ID-->"+logAccoutInfo.getTableId()+"列名："+columnName);
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                jdbc.close();
            }
        }

    }
}
