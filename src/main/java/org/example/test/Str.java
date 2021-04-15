package org.example.test;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/14 16:24
 */
public class Str {
    public static void main(String[] args) {
        System.out.println(isPhoneNum("13111111111"));
        System.out.println(isEmail("aa@12qq.com"));
    }

    public static boolean isPhoneNum(String phone) {
        int length = 11;
        if (phone != null && phone.length() == length) {
            return phone.matches("^1\\d{10}");
        }
        return false;
    }

    public static boolean isEmail(String email) {
        if (email != null) {
            return email.matches("\\w+@\\w+(\\.\\w+)+");
        }
        return false;
    }
}
