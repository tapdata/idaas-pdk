package io.tapdata.connector.tdd;

import java.util.List;

public class TDDUser {
    private String id;
    private String name;
    private String description;
    private int age;
    public static final int GENDER_MALE = 1;
    public static final int GENDER_FEMALE = 10;
    private int gender;

    public TDDUser() {}
    public TDDUser(String id, String name, String description, int age, int gender) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.age = age;
        this.gender = gender;
    }

    private List<String> loginAccounts;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getGender() {
        return gender;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public List<String> getLoginAccounts() {
        return loginAccounts;
    }

    public void setLoginAccounts(List<String> loginAccounts) {
        this.loginAccounts = loginAccounts;
    }
}
