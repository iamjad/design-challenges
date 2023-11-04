package org.designchallenge;

public class Main {
    public static void main(String[] args) {
        UniqueIdGenerator uniqueIdGenerator = new UniqueIdGenerator();
        System.out.println(uniqueIdGenerator.generateShortURLId(uniqueIdGenerator.generateUniqueId()/100000));

        // Create 10 threads and call the Upstream method of WeightedRoutingLB for 100 times.
//        WeightedRoutingLB weightedRoutingLB = new WeightedRoutingLB();
//        for (int i = 0; i < 10; i++) {
//            new Thread(() -> {
//                for (int j = 0; j < 100; j++) {
//                    try {
//                        weightedRoutingLB.callUpstream();
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//            }).start();
//        }
    }
}