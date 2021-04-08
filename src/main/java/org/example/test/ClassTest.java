package org.example.test;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/8 11:23
 */
public class ClassTest {
    public static void main(String[] args) {

        Animal<String, String> animal0 = new Animal<>();

        Animal<String, String> animal1 = new Animal<String, String>() {

        };

        animal0.setT("A");
        animal1.setT("B");

        animal0.fun(new Observe<>());
    }
}

class Animal<T, E> {
    private T t;

    public T getT() {
        return t;
    }

    public void setT(T t) {
        this.t = t;
    }

    public void fun(Observe<E> observe) {
    }

    @Override
    public String toString() {
        return "Animal{" +
                "t=" + t +
                '}';
    }
}

class Observe<E> {
    E t;
}
