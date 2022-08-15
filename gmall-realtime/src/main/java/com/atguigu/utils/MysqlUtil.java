package com.atguigu.utils;

public class MysqlUtil {
    public static String mysqlLookUpTableDDL(String tableName){
        return "WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://192.168.10.100:3306/gmall-flink', " +
                "'table-name' = '" + tableName + "', " +
                "'lookup.cache.max-rows' = '100', " +
                "'lookup.cache.ttl' = '1 hour', " +
                "'username' = 'root', " +
                "'password' = '123456', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }

    public static String getBaseDicLookUpDDL(){
        return "create table `base_dic`( " +
                "    `dic_code` string, " +
                "    `dic_name` string, " +
                "    `parent_code` string, " +
                "    `create_time` timestamp, " +
                "    `operate_time` timestamp, " +
                "    primary key(`dic_code`) not enforced " +
                ")"+MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }
}
