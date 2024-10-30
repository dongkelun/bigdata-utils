package com.dkl.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtils {

    public static String getCurrentTime() {
        // 获取当前日期和时间
        LocalDateTime now = LocalDateTime.now();

        // 打印当前日期和时间
//        System.out.println("当前日期和时间: " + now);

        // 自定义格式输出
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formattedNow = now.format(formatter);
        return formattedNow;
    }

    public static void main(String[] args) {
        System.out.println(TimeUtils.getCurrentTime());
    }
}