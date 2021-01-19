package com.unicom.utils;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.Properties;

public class JdbcUtil {
    private BoneCP connectionPool = null;
    private final byte[] lock = new byte[0]; // 特殊的instance变量
    private static Logger logger = LoggerFactory.getLogger(JdbcUtil.class);
    /**
     * 获取数据库连接池
     *
     * @return 数据库连接池对象
     */
    private BoneCP getConnectionPool(String tableName) {
        synchronized (lock) {
            InputStream inputStream=null;
            try {
                if (tableName.substring(tableName.length() - 4, tableName.length()).equalsIgnoreCase("test")) {
                    inputStream = JdbcUtil.class.getClassLoader().getResourceAsStream("db_test.properties");
                } else {
                    inputStream = JdbcUtil.class.getClassLoader().getResourceAsStream("db.properties");
                }
            }catch(Exception e){
                logger.info("请输入长度大于4的表名");
            }
            Properties properties = new Properties();
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            String driver = properties.getProperty("db.driver", "com.mysql.cj.jdbc.Driver");
            String url = properties.getProperty("db.url");
            String username = properties.getProperty("db.username");
            String password = properties.getProperty("db.password");

            try {
                Class.forName(driver);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }

            BoneCPConfig boneCPConfig = new BoneCPConfig();
            boneCPConfig.setJdbcUrl(url);
            boneCPConfig.setUsername(username);
            boneCPConfig.setPassword(password);
            boneCPConfig.setMaxConnectionsPerPartition(10);
            boneCPConfig.setMinConnectionsPerPartition(1);
            boneCPConfig.setAcquireIncrement(5);
            boneCPConfig.setPoolAvailabilityThreshold(20);
            boneCPConfig.setIdleMaxAge(240, TimeUnit.MINUTES);
            boneCPConfig.setIdleConnectionTestPeriod(60, TimeUnit.MINUTES);
            boneCPConfig.setStatementsCacheSize(20);

            try {
                connectionPool = new BoneCP(boneCPConfig);
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }
        return connectionPool;
    }

    /**
     * 获取连接
     *
     * @return 数据库连接
     */
    public Connection getConnection(String tableName) {
        Connection connection = null;
        synchronized (lock) {
            try {
                if (connectionPool == null)
                    connection = getConnectionPool(tableName).getConnection();
                else
                    connection = connectionPool.getConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * 关闭数据库连接池
     */
    public void close() {
        synchronized (lock) {
            connectionPool.shutdown();
        }
    }

    /**
     *
     */

    /**
     * 重载了方法，
     */
    /**
     * 获取数据库连接池
     *
     * @return 数据库连接池对象
     */
    private BoneCP getConnectionPool() {
        synchronized (lock) {
            InputStream inputStream=null;
            try {
                inputStream = JdbcUtil.class.getClassLoader().getResourceAsStream("db.properties");
            }catch(Exception e){
                logger.info("请输入长度大于4的表名");
            }
            Properties properties = new Properties();
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            String driver = properties.getProperty("db.driver", "com.mysql.cj.jdbc.Driver");
            String url = properties.getProperty("db.url");
            String username = properties.getProperty("db.username");
            String password = properties.getProperty("db.password");

            try {
                Class.forName(driver);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }

            BoneCPConfig boneCPConfig = new BoneCPConfig();
            boneCPConfig.setJdbcUrl(url);
            boneCPConfig.setUsername(username);
            boneCPConfig.setPassword(password);
            boneCPConfig.setMaxConnectionsPerPartition(10);
            boneCPConfig.setMinConnectionsPerPartition(1);
            boneCPConfig.setAcquireIncrement(5);
            boneCPConfig.setPoolAvailabilityThreshold(20);
            boneCPConfig.setIdleMaxAge(240, TimeUnit.MINUTES);
            boneCPConfig.setIdleConnectionTestPeriod(60, TimeUnit.MINUTES);
            boneCPConfig.setStatementsCacheSize(20);

            try {
                connectionPool = new BoneCP(boneCPConfig);
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }
        return connectionPool;
    }

    /**
     * 获取连接
     *
     * @return 数据库连接
     */

    public Connection getConnection() {
        Connection connection = null;
        synchronized (lock) {
            try {
                if (connectionPool == null)
                    connection = getConnectionPool().getConnection();
                else
                    connection = connectionPool.getConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }
}
