package com.oltpbenchmark.benchmarks.tpcc;

import java.util.concurrent.atomic.AtomicInteger;

public class TPCCScheduler implements Runnable {

    public AtomicInteger next_id = new AtomicInteger(10000);

    public int global_counter = 0;

    public void run() {
        if (global_counter == 0) {
            global_counter++;
        }
        //  else {
        //     global_counter--;
        // }
    }

    // private int num_warehouses;

    // public TPCCScheduler(int num_warehouses) {
    //     this.num_warehouses = num_warehouses
    // }

    // public void run() {
    //     if (global_counter < num_warehouses - 1) {
    //         global_counter++;
    //     } else {
    //         global_counter = 0;
    //     }
    // }
}
