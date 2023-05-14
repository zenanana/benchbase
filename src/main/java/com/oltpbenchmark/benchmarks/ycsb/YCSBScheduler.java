package com.oltpbenchmark.benchmarks.ycsb;

public class YCSBScheduler implements Runnable {

    public int global_counter = 0;

    public void run() {
        if (global_counter == 0) {
            global_counter++;
        } else {
            global_counter--;
        }
    }

    // public int get_counter() {
    //     return global_counter;
    // }

    // public static void main(String[] args) {
    //     ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    //     executor.scheduleAtFixedRate(new YCSBScheduler(), 0, 100, TimeUnit.MILLISECONDS);
    // }
}
