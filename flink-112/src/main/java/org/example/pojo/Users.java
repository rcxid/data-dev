package org.example.pojo;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 20:19
 */
public class Users {
    private Integer id;
    private String name;
    private String pwd;

    public Users() {
    }

    public Users(Integer id, String name, String pwd) {
        this.id = id;
        this.name = name;
        this.pwd = pwd;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }
}
