package com.dkl.bigdata.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaDemo {
    Properties getSecurityProperties() {
        return getSecurityProperties(getLoginParam());
    }

    private Properties getSecurityProperties(Map<String, String> param) {
        String principal = param.get("principal");
        String keytab = param.get("keytab");
        String krb5 = param.get("krb5");
        Properties props = new Properties();

        System.setProperty("java.security.krb5.conf", krb5);

        StringBuilder jaas = new StringBuilder("com.sun.security.auth.module.Krb5LoginModule required");
        jaas.append(" useKeyTab=true storeKey=true");
        jaas.append(" keyTab=\"" + keytab + "\"");
        jaas.append(" principal=\"" + principal + "\";");

        props.put("sasl.jaas.config", jaas.toString());
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");
        return props;
    }


    private Map<String, String> getLoginParam() {
        String principal = "kafka/indata-10-110-8-199.indata.com@INDATA.COM";
        String keytab = Thread.currentThread().getContextClassLoader().getResource("kafka.service.keytab").getPath();
        String krb5 = Thread.currentThread().getContextClassLoader().getResource("krb5.conf").getPath();

        Map<String, String> loginParam = new HashMap<>();
        loginParam.put("principal", principal);
        loginParam.put("keytab", keytab);
        loginParam.put("krb5", krb5);
        return loginParam;
    }
}
