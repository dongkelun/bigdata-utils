package com.dkl.bigdata.kafka;

import java.io.File;
import java.util.Map;
import java.util.Properties;

public class SecurityUtils {
    private static final String USER_KEYTAB = "hive.service.keytab";
    private static final String USER_PRINCIPAL = "hive/indata-10-110-105-158.indata.com@INDATA.COM";
    private static final String USER_KRB5 = "krb5.conf";

    public static Properties getSecurityProperties() {

        String basePath = SecurityUtils.class.getResource("/").getFile();
        File config = new File(basePath);
        String keytab = basePath + USER_KEYTAB;
        String krb5 = basePath + USER_KRB5;
        Properties props = new Properties();

        if (USER_KEYTAB.length() != 0 && USER_PRINCIPAL.length() != 0
                && config.exists() && config.isDirectory()) {
            String path = config.getPath().replaceAll("[\\\\/]", "/") + "/";
            System.setProperty("java.security.krb5.conf", krb5);

            StringBuilder jaas = new StringBuilder("com.sun.security.auth.module.Krb5LoginModule required");
            jaas.append(" useKeyTab=true storeKey=true");
            jaas.append(" keyTab=\"" + keytab + "\"");
            jaas.append(" principal=\"" + USER_PRINCIPAL + "\";");

            System.out.println(jaas.toString());
            props.put("sasl.jaas.config", jaas.toString());
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.kerberos.service.name", "kafka");
            props.put("sasl.mechanism", "GSSAPI");

        }

        return props;
    }

    public static Properties getSecurityProperties(Map<String, String> param) {

        String principal = param.get("principal");
        String keytab = param.get("keytab");
        String krb5 = param.get("krb5");
        Properties props = new Properties();

        System.setProperty("java.security.krb5.conf", krb5);

        StringBuilder jaas = new StringBuilder("com.sun.security.auth.module.Krb5LoginModule required");
        jaas.append(" useKeyTab=true storeKey=true");
        jaas.append(" keyTab=\"" + keytab + "\"");
        jaas.append(" principal=\"" + principal + "\";");

        System.out.println(jaas.toString());
        props.put("sasl.jaas.config", jaas.toString());
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");
        return props;
    }

}
