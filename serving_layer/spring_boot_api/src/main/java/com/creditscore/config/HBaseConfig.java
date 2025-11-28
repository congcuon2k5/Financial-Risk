package com.creditscore.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class HBaseConfig {

    @Value("${hbase.zookeeper.quorum:hbase}")
    private String zkQuorum;

    @Value("${hbase.zookeeper.port:2181}")
    private int zkPort;

    @Bean
    public org.apache.hadoop.conf.Configuration hbaseConfiguration() {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkQuorum);
        config.set("hbase.zookeeper.property.clientPort", String.valueOf(zkPort));
        // nếu trong hbase-site.xml là /hbase-unsecure thì sửa dòng dưới
        config.set("zookeeper.znode.parent", "/hbase");
        return config;
    }

    @Bean(destroyMethod = "close")
    public Connection hbaseConnection(org.apache.hadoop.conf.Configuration configuration) throws IOException {
        return ConnectionFactory.createConnection(configuration);
    }
}
