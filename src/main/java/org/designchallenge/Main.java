package org.designchallenge;

public class Main {
    public static void main(String[] args) {
        UniqueIdGenerator uniqueIdGenerator = new UniqueIdGenerator();
        System.out.println(uniqueIdGenerator.generateShortURLId(uniqueIdGenerator.generateUniqueId()));


    }
}