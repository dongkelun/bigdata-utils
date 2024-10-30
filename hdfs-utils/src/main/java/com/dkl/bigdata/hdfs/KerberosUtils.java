package com.dkl.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KerberosUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KerberosUtils.class);

    private static String confDir = "D:/data/config/199/";
    private static String localKrb5 = confDir + "krb5.conf";
    private static String localKeyTabPath = confDir + "hive.service.keytab";
    private static String localPrincipal = "hive/indata-10-110-8-199.indata.com@INDATA.COM";


    public static void initKerberosConf(String krb5ConfFilePath, String keytabFilePath, String kerberosUser) {
        System.setProperty("java.security.krb5.conf", krb5ConfFilePath);
//        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
//        System.setProperty("sun.security.krb5.debug", "false");
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(kerberosUser, keytabFilePath);
            LOG.info("kerberos认证成功！");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 初始化Kerberos环境
     *
     * @param conf
     * @param krb5ConfFilePath
     * @param kerberosUser
     * @param keytabFilePath
     */
    private static boolean initKerberosConf(Configuration conf, String krb5ConfFilePath, String keytabFilePath, String kerberosUser) {
        System.setProperty("java.security.krb5.conf", krb5ConfFilePath);
//        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
//        System.setProperty("sun.security.krb5.debug", "false");

        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(kerberosUser, keytabFilePath);
            if (!UserGroupInformation.isLoginKeytabBased()) {
                throw new KerberosAuthException("不是基于kerberos认证， 请检查配置文件是否正确");
            }
            return UserGroupInformation.isLoginKeytabBased();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Configuration initKerberosAndGetConf(String envType, String keyTabPath, String principal) {
        String krb5;
        Configuration hdfsConf;

        if ("local".equals(envType)) {
            hdfsConf = HdfsConfigUtils.initConfiguration(confDir);
            krb5 = localKrb5;
            keyTabPath = localKeyTabPath;
            principal = localPrincipal;
        } else {
            krb5 = "/etc/krb5.conf";
            hdfsConf = HdfsConfigUtils.initConfiguration("/etc/hadoop/conf");
        }
        if (initKerberosConf(hdfsConf, krb5, keyTabPath, principal)) {
            LOG.info("kerberos认证成功！");
        } else {
            throw new RuntimeException("kerberos认证失败！");
        }
        return hdfsConf;
    }

    public static void main(String[] args) {
        initKerberosAndGetConf("local", localKeyTabPath, localPrincipal);
        initKerberosConf(localKrb5, localKeyTabPath, localPrincipal);
    }
}
