package com.zql.KafkaLearn.common;

public class User {
    private String name;
    private int age;
    private String address;

    public User(String name, int age, String address) {
        super();
        this.name = name;
        this.age = age;
        this.address = address;
    }

    @Override
    public String toString() {
        return "User [name=" + name + ", age=" + age + ", address=" + address + "]";
    }
}
