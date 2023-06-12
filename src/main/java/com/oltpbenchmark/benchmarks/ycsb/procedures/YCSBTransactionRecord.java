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

// package com.oltpbenchmark.benchmarks.ycsb.custom_procedures;
package com.oltpbenchmark.benchmarks.ycsb.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.oltpbenchmark.benchmarks.ycsb.YCSBConstants.TABLE_NAME;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;


/*
Notation for X, Y, Z:
X: first set of hotkeys
Y: large filler set of keys
Z: second set of hotkeys
*/
public class YCSBTransactionRecord extends Procedure {
    /**
     * get random integer in range [min, max]
     */
    public int randInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    public final SQLStmt selectXStmt = new SQLStmt(
        "SELECT * FROM " + TABLE_NAME + " where YCSB_KEY=?"
    );

    public final SQLStmt selectZStmt = new SQLStmt(
        "SELECT * FROM " + TABLE_NAME + " where YCSB_KEY=? FOR UPDATE"
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
     * X: single hotkey from group 1
     * Z: single hotkey from group 2
     * Y_start, Y_end: range of filler keys to use for filler stmts
     */
    public void run(Connection conn, Integer[] trx_args, ArrayList<Integer> keys, String[] fields, String[] results) throws SQLException {
        // TODO: get Z_start and Z_end within function rather than passing as arguments
        // System.out.printf("Trying to start cluster 1 with %d and %d%n", X, Z);

        ArrayList<Integer> hot_keys = new ArrayList<>();
        hot_keys.add(0);
        hot_keys.add(1);
        hot_keys.add(2);
        hot_keys.add(3);
        hot_keys.add(4);
        hot_keys.add(5);
        hot_keys.add(6);
        hot_keys.add(7);
        hot_keys.add(8);
        hot_keys.add(9);

        List<Integer> present_hot_keys = new ArrayList<>();
        List<Integer> other_keys = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            if (hot_keys.contains(keys.get(i))) {
                present_hot_keys.add(keys.get(i));
            } else {
                other_keys.add(keys.get(i));
            }
        }
        // System.out.println("keys: " + other_keys.toString() + " present_hot_keys: " + present_hot_keys.toString());
        java.util.Collections.shuffle(present_hot_keys);

        ArrayList<Integer> first_half = new ArrayList<>();
        // for (int i = 0; i < present_hot_keys.size(); i++) {
        //     first_half.add(present_hot_keys.get(i));
        // }
        ArrayList<Integer> second_half = new ArrayList<>();
        // int count = 0;
        // while (first_half.size() > 0 && count < 2) {
        //     second_half.add(first_half.remove(first_half.size() - 1));
        //     count++;
        // }
        // for (int i = present_hot_keys.size() - 2; i < present_hot_keys.size(); i++) {
        //     second_half.add(present_hot_keys.get(i));
        // }

        // int type = 0;
        // if (second_half.contains(0)) {
        //     type = 0;
        // } else {
        //     type = 1;
        // }

        // int type = 0;
        // if (present_hot_keys.contains(0)) {
        //     // if (present_hot_keys.indexOf(0) > present_hot_keys.size() / 2) {
        //     //     type = 1;
        //     //     second_half.add(0);
        //     // }
        //     if (type == 0) {
        //         first_half.add(0);
        //     } else {
        //         second_half.add(0);
        //     }
        // }
        // // int type = randInt(0,1);
        // // if (type == 0) {
        // //         first_half.add(0);
        // //     } else {
        // //         second_half.add(0);
        // //     }
        // if (present_hot_keys.contains(1)) {
        //     if (type == 0) {
        //         second_half.add(1);
        //     } else {
        //         first_half.add(1);
        //     }
        // }
        // if (present_hot_keys.contains(2)) {
        //     if (type == 0) {
        //         first_half.add(2);
        //     } else {
        //         second_half.add(2);
        //     }
        // }
        // if (present_hot_keys.contains(3)) {
        //     if (type == 0) {
        //         second_half.add(3);
        //     } else {
        //         first_half.add(3);
        //     }
        // }
        int type = randInt(0,3);
        if (present_hot_keys.contains(0)) {
            if (type == 0) {
                second_half.add(0);
                second_half.add(4);
                second_half.add(8);
            } else {
                first_half.add(0);
                first_half.add(4);
                first_half.add(8);
            }
        }
        if (present_hot_keys.contains(1)) {
            if (type == 1) {
                second_half.add(1);
                second_half.add(5);
                second_half.add(9);
            } else {
                first_half.add(1);
                first_half.add(5);
                first_half.add(9);
            }
        }
        if (present_hot_keys.contains(2)) {
            if (type == 2) {
                second_half.add(2);
                second_half.add(6);
            } else {
                first_half.add(2);
                first_half.add(6);
            }
        }
        if (present_hot_keys.contains(3)) {
            if (type == 3) {
                second_half.add(3);
                second_half.add(7);
            } else {
                first_half.add(3);
                first_half.add(7);
            }
        }
        // System.out.printf("type %d first_half %s second_half %s%n", type, first_half.toString(), second_half.toString());

        // Start trx for stmt
        try (PreparedStatement stmt = this.getPreparedStatement(conn, startTrxForStmt)) {
            stmt.setInt(1, type);
            stmt.setInt(2, trx_args[0]);
            stmt.setInt(3, trx_args[1]);
            stmt.execute();
        }
        // System.out.println("Start cluster 1 done");

        // Fetch it!
        for (int i = 0; i < first_half.size(); i++) {
            // System.out.printf("First half %d%n", first_half.get(i));
            try (PreparedStatement stmt = this.getPreparedStatement(conn, selectXStmt)) {
                stmt.setInt(1, first_half.get(i));
                try (ResultSet r = stmt.executeQuery()) {
                    while (r.next()) {
                        for (int j = 0; j < YCSBConstants.NUM_FIELDS; j++) {
                            results[j] = r.getString(j + 1);
                        }
                    }
                }
            }
        }
        // System.out.printf("Read cluster 1 to %d done%n", X);

        // try (PreparedStatement stmt = this.getPreparedStatement(conn, updateZStmt)) {
        //     stmt.setInt(11, X);

        //     for (int i = 0; i < fields.length; i++) {
        //         stmt.setString(i + 1, fields[i]);
        //     }
        //     stmt.executeUpdate();
        // }
        // System.out.printf("Write cluster 1 to %d done%n", X);

        Set<Integer> set = new HashSet<Integer>();
        // Bunch of filler stmts
        for (int i = 0; i < other_keys.size(); i++) {
            if (randInt(0,1) < 1) {
                try (PreparedStatement stmt = this.getPreparedStatement(conn, fillerYStmt)) {
                    stmt.setInt(1, other_keys.get(i));
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
                    // TODO(accheng): update this when we fix rollback bug
                    stmt.setInt(11, other_keys.get(i));
                    // int randInt = randInt(YCSBConstants.RECORD_COUNT + 1, 500000);
                    // stmt.setInt(11, randInt);
                    for (int j = 0; j < fields.length; j++) {
                        stmt.setString(j + 1, fields[j]);
                    }
                    stmt.executeUpdate();
                }
            }
        }

        // try (PreparedStatement stmt = this.getPreparedStatement(conn, selectZStmt)) {
        //     stmt.setInt(1, Z);
        //     try (ResultSet r = stmt.executeQuery()) {
        //         while (r.next()) {
        //             for (int i = 0; i < YCSBConstants.NUM_FIELDS; i++) {
        //                 results[i] = r.getString(i + 1);
        //             }
        //         }
        //     }
        // }
        // System.out.printf("Read2 cluster 1 to %d done%n", Z);

        // Update that mofo
        for (int i = 0; i < second_half.size(); i++) {
            // System.out.printf("Second half %d%n", second_half.get(i));
            try (PreparedStatement stmt = this.getPreparedStatement(conn, updateZStmt)) {
                stmt.setInt(11, second_half.get(i));

                for (int j = 0; j < fields.length; j++) {
                    stmt.setString(j + 1, fields[j]);
                }
                stmt.executeUpdate();
            }
        }
        // System.out.printf("Write2 cluster 1 to %d done%n", X);

        // // Commit trx
        // try (PreparedStatement stmt = this.getPreparedStatement(conn, commitStmt)) {
        //     stmt.execute();
        // }
    }

}
