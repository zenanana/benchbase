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

package com.oltpbenchmark.benchmarks.ycsb;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ycsb.procedures.*;
import com.oltpbenchmark.distributions.CounterGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.TextGenerator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

/**
 * YCSBWorker Implementation
 * I forget who really wrote this but I fixed it up in 2016...
 *
 * @author pavlo
 */
public class YCSBWorker extends Worker<YCSBBenchmark> {
    private YCSBScheduler scheduler;

    private final ZipfianGenerator readRecord;
    private static CounterGenerator insertRecord;
    private final ZipfianGenerator randScan;

    private final char[] data;
    private final String[] params = new String[YCSBConstants.NUM_FIELDS];
    private final String[] results = new String[YCSBConstants.NUM_FIELDS];

    private final UpdateRecord procUpdateRecord;
    private final ScanRecord procScanRecord;
    private final ReadRecord procReadRecord;
    private final ReadModifyWriteRecord procReadModifyWriteRecord;
    private final InsertRecord procInsertRecord;
    private final DeleteRecord procDeleteRecord;

    /* START CUSTOM PROCEDURES */
    private final ReadXWriteZRecord procReadXWriteZRecord;
    private final ReadZWriteXRecord procReadZWriteXRecord;
    /* END CUSTOM PROCEDURES */

    public YCSBWorker(YCSBBenchmark benchmarkModule, int id, int init_record_count) {
        super(benchmarkModule, id);
        this.data = new char[benchmarkModule.fieldSize];
        this.readRecord = new ZipfianGenerator(rng(), init_record_count);// pool for read keys
        this.randScan = new ZipfianGenerator(rng(), YCSBConstants.MAX_SCAN);

        synchronized (YCSBWorker.class) {
            // We must know where to start inserting
            if (insertRecord == null) {
                insertRecord = new CounterGenerator(init_record_count);
            }
        }

        // This is a minor speed-up to avoid having to invoke the hashmap look-up
        // everytime we want to execute a txn. This is important to do on
        // a client machine with not a lot of cores
        this.procUpdateRecord = this.getProcedure(UpdateRecord.class);
        this.procScanRecord = this.getProcedure(ScanRecord.class);
        this.procReadRecord = this.getProcedure(ReadRecord.class);
        this.procReadModifyWriteRecord = this.getProcedure(ReadModifyWriteRecord.class);
        this.procInsertRecord = this.getProcedure(InsertRecord.class);
        this.procDeleteRecord = this.getProcedure(DeleteRecord.class);

        /* START CUSTOM PROCEDURES */
        this.procReadXWriteZRecord = this.getProcedure(ReadXWriteZRecord.class);
        this.procReadZWriteXRecord = this.getProcedure(ReadZWriteXRecord.class);
        /* END CUSTOM PROCEDURES */
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans) throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();

        if (procClass.equals(DeleteRecord.class)) {
            deleteRecord(conn);
        } else if (procClass.equals(InsertRecord.class)) {
            insertRecord(conn);
        } else if (procClass.equals(ReadModifyWriteRecord.class)) {
            readModifyWriteRecord(conn);
        } else if (procClass.equals(ReadRecord.class)) {
            readRecord(conn);
        } else if (procClass.equals(ScanRecord.class)) {
            scanRecord(conn);
        } else if (procClass.equals(UpdateRecord.class)) {
            updateRecord(conn);
        }

        /* START CUSTOM PROCEDURES */
        // if (scheduler.global_counter == 0) {
        //     readZWriteXRecord(conn);
        // } else {
        //     readXWriteZRecord(conn);
        // }

        if (procClass.equals(ReadZWriteXRecord.class)) {
            int count = scheduler.global_counter;
            while(count != 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {}
                count = scheduler.global_counter;
            }
            readZWriteXRecord(conn);
        } else if (procClass.equals(ReadXWriteZRecord.class)) {
            int count = scheduler.global_counter;
            while(count != 1) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {}
                count = scheduler.global_counter;
            }
            readXWriteZRecord(conn);
        }
        /* END CUSTOM PROCEDURES */

        return (TransactionStatus.SUCCESS);
    }

    public void set_scheduler(YCSBScheduler scheduler) {
        this.scheduler = scheduler;
    }

    private void updateRecord(Connection conn) throws SQLException {

        int keyname = readRecord.nextInt();
        this.buildParameters();
        this.procUpdateRecord.run(conn, keyname, this.params);
    }

    private void scanRecord(Connection conn) throws SQLException {

        int keyname = readRecord.nextInt();
        int count = randScan.nextInt();
        this.procScanRecord.run(conn, keyname, count, new ArrayList<>());
    }

    private void readRecord(Connection conn) throws SQLException {

        int keyname = readRecord.nextInt();
        this.procReadRecord.run(conn, keyname, this.results);
    }

    private void readModifyWriteRecord(Connection conn) throws SQLException {

        int keyname = readRecord.nextInt();
        this.buildParameters();
        this.procReadModifyWriteRecord.run(conn, keyname, this.params, this.results);
    }

    private void insertRecord(Connection conn) throws SQLException {

        int keyname = insertRecord.nextInt();
        this.buildParameters();
        this.procInsertRecord.run(conn, keyname, this.params);
    }

    private void deleteRecord(Connection conn) throws SQLException {

        int keyname = readRecord.nextInt();
        this.procDeleteRecord.run(conn, keyname);
    }

    /* START CUSTOM PROCEDURES */
    private void readXWriteZRecord(Connection conn) throws SQLException {
        int key_X = readRecord.nextStartingHotkey(YCSBConstants.HOTKEY_SET_SIZE);
        int key_Z = readRecord.nextEndingHotkey(YCSBConstants.HOTKEY_SET_SIZE);
        int Y_start = readRecord.fillerKeyStart(YCSBConstants.HOTKEY_SET_SIZE);
        int Y_end = readRecord.fillerKeyEnd(YCSBConstants.HOTKEY_SET_SIZE);

        // System.out.println("ReadXWriteZ: key_X: " + key_X + " | key_Z: " + key_Z + " | key_Y_start: " + Y_start + " | key_Y_end: " + Y_end + "\n");

        Integer[] placeholder = {0, 0};
        if (key_X == 0) {
            placeholder[0] = 0;
        } else {
            placeholder[0] = 2;
        }
        if (key_Z == 999) {
            placeholder[1] = 7;
        } else {
            placeholder[1] = 5;
        }
        this.buildParameters();
        this.procReadXWriteZRecord.run(conn, 0 /* type */, placeholder, key_X, key_Z, Y_start, Y_end, this.params, this.results); // TODO: replace start for trx argument placeholders
    }

    private void readZWriteXRecord(Connection conn) throws SQLException {
        int key_X = readRecord.nextStartingHotkey(YCSBConstants.HOTKEY_SET_SIZE);
        int key_Z = readRecord.nextEndingHotkey(YCSBConstants.HOTKEY_SET_SIZE);
        int Y_start = readRecord.fillerKeyStart(YCSBConstants.HOTKEY_SET_SIZE);
        int Y_end = readRecord.fillerKeyEnd(YCSBConstants.HOTKEY_SET_SIZE);

        // System.out.println("ReadZWriteX: key_X: " + key_X + " | key_Z: " + key_Z + " | key_Y_start: " + Y_start + " | key_Y_end: " + Y_end + "\n");

        Integer[] placeholder = {0, 0};
        if (key_X == 0) {
            placeholder[0] = 1;
        } else {
            placeholder[0] = 3;
        }
        if (key_Z == 999) {
            placeholder[1] = 6;
        } else {
            placeholder[1] = 4;
        }
        this.buildParameters();
        this.procReadZWriteXRecord.run(conn, 1 /* type */, placeholder, key_X, key_Z, Y_start, Y_end, this.params, this.results); // TODO: replace start for trx argument placeholders
    }
    /* END CUSTOM PROCEDURES */


    private void buildParameters() {
        for (int i = 0; i < this.params.length; i++) {
            this.params[i] = new String(TextGenerator.randomFastChars(rng(), this.data));
        }
    }
}
