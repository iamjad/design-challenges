package org.designchallenge;

import java.util.concurrent.atomic.AtomicInteger;

public class WeightedRoutingLB {

    public static int TRAFFIC_DISTRIBUTION = 10; // 10% of the traffic
    private AtomicInteger trafficDistributionCounter = new AtomicInteger(0); // 10% of the traffic

    public void callUpstream() throws InterruptedException {
        int traffic = trafficDistributionCounter.incrementAndGet();
        if (traffic == 100) {
            trafficDistributionCounter.set(1);
            traffic = 1;
        }
        if (traffic <= TRAFFIC_DISTRIBUTION) {
            System.out.println("Traffic to : " + callUpstreamA() + " " + traffic);
        } else {
            System.out.println("Traffic to : " + callUpstreamB() + " " + traffic);
        }
    }

    private String callUpstreamA() throws InterruptedException {
        Thread.sleep(80);
        return "callUpstreamA";
    }

    private String callUpstreamB() throws InterruptedException {
        Thread.sleep(100);
        return "callUpstreamB";
    }
}
