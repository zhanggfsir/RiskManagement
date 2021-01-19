建表
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.CreateTable 319 user_daily_msisdn

插入
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.InsertTable 319 user_daily_msisdn ui 20190607

查询
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.QueryTable 319 user_daily_msisdn ui 262CA5841FCF3D2936646608F90AEE61 20190606

compact
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.MajorCompactTable 319 user_daily_msisdn

删除
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.DropTable  319 user_daily_msisdn

重命名
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.RenameTable 319 user_daily_msisdn user_daily_msisdn_rename

