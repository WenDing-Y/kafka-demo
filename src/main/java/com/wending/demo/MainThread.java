package com.wending.demo;

/**
 * Created by shuiyu lei
 * date 2019/4/4
 */
public class MainThread {

    public static void main(String[] args) {
        Producer producer = new Producer("order",false);
        producer.start();
    }
}
