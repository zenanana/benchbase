/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.benchmarks.tpcc;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.procedures.TPCCProcedure;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.types.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TPCCWorker extends Worker<TPCCBenchmark> {
    /**
     * get random integer in range [min, max]
     */
    public int randInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    private TPCCScheduler scheduler;

    private static final Logger LOG = LoggerFactory.getLogger(TPCCWorker.class);

    private final int terminalWarehouseID;
    /**
     * Forms a range [lower, upper] (inclusive).
     */
    private final int terminalDistrictLowerID;
    private final int terminalDistrictUpperID;
    private final Random gen = new Random();

    private final int numWarehouses;

    private int schedule;

    public TPCCWorker(TPCCBenchmark benchmarkModule, int id,
                      int terminalWarehouseID, int terminalDistrictLowerID,
                      int terminalDistrictUpperID, int numWarehouses,
                      int schedule) {
        super(benchmarkModule, id);

        this.terminalWarehouseID = terminalWarehouseID;
        this.terminalDistrictLowerID = terminalDistrictLowerID;
        this.terminalDistrictUpperID = terminalDistrictUpperID;

        this.numWarehouses = numWarehouses;

        this.schedule = schedule;
    }

    public void set_scheduler(TPCCScheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * Executes a single TPCC transaction of type transactionType.
     */
    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction) throws UserAbortException, SQLException {
        try {
            TPCCProcedure proc = (TPCCProcedure) this.getProcedure(nextTransaction.getProcedureClass());
            // if (randInt(0,100) < 80 && this.id == 0) {
            //     while (proc.toString() != "Delivery") {
            //         proc = (TPCCProcedure) this.getProcedure(nextTransaction.getProcedureClass());
            //     }
            // }
            // if (proc.toString() == "NewOrder") {
                // int next_id = scheduler.next_id.getAndIncrement();
                // System.out.printf("next_id: %d%n",next_id);
                proc.run(conn, gen, terminalWarehouseID, numWarehouses, 0, //next_id, //
                    terminalDistrictLowerID, terminalDistrictUpperID, this.schedule, this);
            //     conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            // } else {
                // conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            // }
            //     int count = scheduler.global_counter;
            //     while(count != 0) {
            //         try {
            //             Thread.sleep(10);
            //         } catch (InterruptedException e) {}
            //         count = scheduler.global_counter;
            //     }
            // } else if (proc.toString() == "Payment") {
            //     int count = scheduler.global_counter;
            //     while(count != 1) {
            //         try {
            //             Thread.sleep(10);
            //         } catch (InterruptedException e) {}
            //         count = scheduler.global_counter;
            //     }
            // }
            // System.out.println("running " + proc.toString());
            // proc.run(conn, gen, terminalWarehouseID, numWarehouses,
            //         terminalDistrictLowerID, terminalDistrictUpperID, this);
            // }
        } catch (ClassCastException ex) {
            //fail gracefully
            LOG.error("We have been invoked with an INVALID transactionType?!", ex);
            throw new RuntimeException("Bad transaction type = " + nextTransaction);
        }
        return (TransactionStatus.SUCCESS);
    }

    @Override
    protected long getPreExecutionWaitInMillis(TransactionType type) {
        // TPC-C 5.2.5.2: For keying times for each type of transaction.
        return type.getPreExecutionWait();
    }

    @Override
    protected long getPostExecutionWaitInMillis(TransactionType type) {
        // TPC-C 5.2.5.4: For think times for each type of transaction.
        long mean = type.getPostExecutionWait();

        float c = this.getBenchmark().rng().nextFloat();
        long thinkTime = (long) (-1 * Math.log(c) * mean);
        if (thinkTime > 10 * mean) {
            thinkTime = 10 * mean;
        }

        return thinkTime;
    }

}
