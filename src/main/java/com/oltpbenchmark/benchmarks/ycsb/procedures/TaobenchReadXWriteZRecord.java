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

package com.oltpbenchmark.benchmarks.ycsb.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.oltpbenchmark.benchmarks.ycsb.YCSBConstants.TABLE_NAME;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;


public class TaobenchReadXWriteZRecord extends Procedure {
    /**
     * get random integer in range [min, max]
     */
    public int randInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    public final SQLStmt selectXStmt = new SQLStmt(
        "SELECT * FROM " + TABLE_NAME + " where YCSB_KEY=?" //
    );

    public final SQLStmt selectXUpdateStmt = new SQLStmt(
        "SELECT * FROM " + TABLE_NAME + " where YCSB_KEY=? FOR UPDATE" //
    );

    public final SQLStmt updateZStmt = new SQLStmt(
        "UPDATE " + TABLE_NAME + " SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
                "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?"
    );

    public final SQLStmt fillerYStmt = new SQLStmt(
        "SELECT * FROM " + TABLE_NAME + " where YCSB_KEY=?"
    );

    public final SQLStmt startTrxForStmt = new SQLStmt(
        YCSBConstants.START_TRX_FOR_STMT
    );

    public final SQLStmt commitStmt = new SQLStmt(
        YCSBConstants.COMMIT_TRX_STMT
    );

    //FIXME: The value in ysqb is a byteiterator
    /**
     * trx_typ: first argument for START FOR
     * trx_args: remaining arguments for START FOR
     * read_keys: keys to read from
     * write_keys: keys to write to
     */
    public void run(Connection conn, Integer[] trx_args, //int[] read_keys, int[] write_keys,
        int Z_start, int Z_end, String[] fields, String[] results) throws SQLException {
        // Prepare sets of read and write keys
        // float read_ratio = 0.5f;
        // List<Integer> read_hotkeys = new ArrayList<Integer>((int) Math.round(read_keys.length * read_ratio));
        // List<Integer> read_nonhotkeys = new ArrayList<Integer>(read_keys.length - (int) Math.round(read_keys.length * read_ratio));
        // for (int i = 0; i < read_keys.length; i++) {
        //     if (i < read_keys.length / 2) {
        //         read_hotkeys.add(read_keys[i]);
        //     } else {
        //         read_nonhotkeys.add(read_keys[i]);
        //     }
        // }
        // java.util.Collections.shuffle(read_hotkeys);
        // java.util.Collections.shuffle(read_nonhotkeys);

        // List<Integer> write_nonhotkeys = new ArrayList<Integer>(write_keys.length);
        // for (int i = 0; i < write_keys.length; i++) {
        //     write_nonhotkeys.add(write_keys[i]);
        // }
        // java.util.Collections.shuffle(write_nonhotkeys);

        int[] read_keys = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        int[] write_keys = {0};

        int type = randInt(0,7);
        boolean finalWrite = false;
        int percent = randInt(0,99);
        if (percent < 35) { // 15 50 65 43 35 27
            finalWrite = true;
            if (type == 1) {
                write_keys[0] = 1;
            } else if (type == 2) {
                write_keys[0] = 2;
            } else if (type == 3) {
                write_keys[0] = 3;
            } else if (type == 4) {
                write_keys[0] = 4;
            } else if (type == 5) {
                write_keys[0] = 5;
            } else if (type == 6) {
                write_keys[0] = 6;
            } else if (type == 7) {
                write_keys[0] = 7;
            }
            // } else if (type == 8) {
            //     write_keys[0] = 8;
            // } else if (type == 9) {
            //     write_keys[0] = 9;
            // }

            if (randInt(0,99) < 1) {
                type = randInt(0,7);
            }
        } else {
            type = 11;
        }
        // } else if (percent < 70) {
        //     type = 11;
        // } else {
        //     type = 12;
        //     write_keys[0] = randInt(0,7);
        // }

        // else if (type == 6) {
        //     write_keys[0] = 6;
        // } else if (type == 7) {
        //     write_keys[0] = 7;
        // } else if (type == 8) {
        //     write_keys[0] = 8;
        // } else if (type == 9) {
        //     write_keys[0] = 9;
        // }

        // if (type == 0) {
        //     if (randInt(0,1) == 0) {
        //         read_keys[0] = 0;
        //         read_keys[1] = 1000;
        //         write_keys[0] = 0;
        //     } else {
        //         read_keys[0] = 0;
        //         read_keys[1] = 999;
        //         write_keys[0] = 0;
        //     }
        // } else if (type == 1) {
        //     if (randInt(0,1) == 0) {
        //         read_keys[0] = 1;
        //         read_keys[1] = 1000;
        //         write_keys[0] = 1;
        //     } else {
        //         read_keys[0] = 1;
        //         read_keys[1] = 999;
        //         write_keys[0] = 1;
        //     }
        // } else if (type == 2) {
        //     if (randInt(0,1) == 0) {
        //         read_keys[0] = 0;
        //         read_keys[1] = 999;
        //         write_keys[0] = 999;
        //     } else {
        //         read_keys[0] = 1;
        //         read_keys[1] = 999;
        //         write_keys[0] = 999;
        //     }
        // } else if (type == 3) {
        //     if (randInt(0,1) == 0) {
        //         read_keys[0] = 0;
        //         read_keys[1] = 1000;
        //         write_keys[0] = 1000;
        //     } else {
        //         read_keys[0] = 1;
        //         read_keys[1] = 1000;
        //         write_keys[0] = 1000;
        //     }
        // }
        int num_cold_keys = randInt(20, 55);//85);
        if (!finalWrite) { //  || type != 12
        if (randInt(0,1) < 1) {
            num_cold_keys = randInt(1,10);
        }
        }
        // System.out.printf("Start taobench type %d start %d end %d write %d%n", type, Z_start, Z_end, write_keys[0]);
        //  System.out.printf("Type %d start %d end %d write %d%n", type, read_keys[0], read_keys[1], write_keys[0]);

        // START TRANSACTION
        // Start trx for stmt
        if (type < 10) { //  || type == 12
            try (PreparedStatement stmt = this.getPreparedStatement(conn, startTrxForStmt)) {
                stmt.setInt(1, write_keys[0]+1); //type+1); //
                stmt.setInt(2, trx_args[0]);
                stmt.setInt(3, trx_args[1]);
                stmt.execute();
            }
        }

        // Read from set of read hotkeys
        if (finalWrite || type == 11) { // == 12
        for (int i = 0; i < read_keys.length; i++) {
            boolean willWrite = false;
            for (int wk : write_keys) {
                if (wk == read_keys[i]) {
                    willWrite = true;
                    break;
                }
            }
            if (!willWrite) {
                // System.out.printf("Read 1 %d%n", read_keys[i]);
                try (PreparedStatement stmt = this.getPreparedStatement(conn, selectXStmt)) {
                    stmt.setInt(1, read_keys[i]);
                    try (ResultSet r = stmt.executeQuery()) {
                        while (r.next()) {
                            for (int j = 0; j < YCSBConstants.NUM_FIELDS; j++) {
                                results[j] = r.getString(j + 1);
                            }
                        }
                    }
                }
            }
        }
        }

        if (finalWrite) {
        Set<Integer> set = new HashSet<Integer>();
        // Bunch of filler stmts
        for (int i = 0; i < num_cold_keys; i++) {
            int randInt = randInt(YCSBConstants.RECORD_COUNT + 1, 500000);
            while (set.contains(randInt)) {
                randInt = randInt(YCSBConstants.RECORD_COUNT + 1, 500000);
            }
            set.add(randInt);

            if (randInt % 10 < 8) {
                try (PreparedStatement stmt = this.getPreparedStatement(conn, fillerYStmt)) {
                    stmt.setInt(1, randInt);
                    try (ResultSet r = stmt.executeQuery()) {
                        while (r.next()) {
                            for (int j = 0; j < YCSBConstants.NUM_FIELDS; j++) {
                                results[j] = r.getString(j + 1);
                            }
                        }
                    }
                }
            } else {
                try (PreparedStatement stmt = this.getPreparedStatement(conn, updateZStmt)) {
                    stmt.setInt(11, randInt);

                    for (int j = 0; j < fields.length; j++) {
                        stmt.setString(j + 1, fields[j]);
                    }
                    stmt.executeUpdate();
                }
            }
        }
        }

        if (finalWrite) { //  || type >= 11  || type == 12
        for (int i = 0; i < read_keys.length; i++) {
            boolean willWrite = false;
            for (int wk : write_keys) {
                if (wk == read_keys[i]) {
                    willWrite = true;
                    break;
                }
            }
            if (willWrite) {
                // System.out.printf("Read 2 %d%n", read_keys[i]);
                try (PreparedStatement stmt = this.getPreparedStatement(conn, selectXUpdateStmt)) {
                    stmt.setInt(1, read_keys[i]);
                    try (ResultSet r = stmt.executeQuery()) {
                        while (r.next()) {
                            for (int j = 0; j < YCSBConstants.NUM_FIELDS; j++) {
                                results[j] = r.getString(j + 1);
                            }
                        }
                    }
                }
            }
        }
        }

        // Write to set of hot keys
        if (finalWrite) { // == 11 //  || type >= 11  || type == 12
            for (int i = 0; i < write_keys.length; i++) {
                // System.out.printf("Write %d%n", write_keys[i]);
                try (PreparedStatement stmt = this.getPreparedStatement(conn, updateZStmt)) {
                    stmt.setInt(11, write_keys[i]);

                    for (int j = 0; j < fields.length; j++) {
                        stmt.setString(j + 1, fields[j]);
                    }
                    stmt.executeUpdate();
                }
            }
        }

        // // Commit trx
        // try (PreparedStatement stmt = this.getPreparedStatement(conn, commitStmt)) {
        //     stmt.execute();
        // }
    }

}
