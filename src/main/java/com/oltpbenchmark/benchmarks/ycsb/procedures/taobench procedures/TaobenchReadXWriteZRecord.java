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


public class TaobenchReadXWriteZRecord extends Procedure {
    /**
     * get random integer in range [min, max]
     */
    public int randInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    public final SQLStmt selectXStmt = new SQLStmt(
        "SELECT * FROM " + TABLE_NAME + " where YCSB_KEY=?" //  FOR UPDATE
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
    public void run(Connection conn, int trx_typ, Integer[] trx_args, int[] read_keys, int[] write_keys, String[] fields, String[] results) throws SQLException {
        // Prepare sets of read and write keys
        float read_ratio = 0.5f;
        List<Integer> read_hotkeys = new ArrayList<Integer>((int) Math.round(read_keys.length * read_ratio));
        List<Integer> read_nonhotkeys = new ArrayList<Integer>(read_keys.length - (int) Math.round(read_keys.length * read_ratio));
        for (int i = 0; i < read_keys.length; i++) {
            if (i < read_keys.length / 2) {
                read_hotkeys.add(read_keys[i]);
            } else {
                read_nonhotkeys.add(read_keys[i]);
            }
        }
        java.util.Collections.shuffle(read_hotkeys);
        java.util.Collections.shuffle(read_nonhotkeys);

        List<Integer> write_nonhotkeys = new ArrayList<Integer>(write_keys.length);
        for (int i = 0; i < write_keys.length; i++) {
            write_nonhotkeys.add(write_keys[i]);
        }
        java.util.Collections.shuffle(write_nonhotkeys);

        // START TRANSACTION
        // Start trx for stmt
        try (PreparedStatement stmt = this.getPreparedStatement(conn, startTrxForStmt)) {
            stmt.setInt(1, trx_typ);
            stmt.setInt(2, trx_args[0]);
            stmt.setInt(3, trx_args[1]);
            stmt.execute();
        }

        // Read from set of read hotkeys
        for (int i = 0; i < read_hotkeys.size(); i++) {
            try (PreparedStatement stmt = this.getPreparedStatement(conn, selectXStmt)) {
                stmt.setInt(1, read_hotkeys.get(i));
                try (ResultSet r = stmt.executeQuery()) {
                    while (r.next()) {
                        for (int j = 0; j < YCSBConstants.NUM_FIELDS; j++) {
                            results[j] = r.getString(j + 1);
                        }
                    }
                }
            }
        }

        // Read from set of read non-hotkeys
        for (int i = 0; i < read_nonhotkeys.size(); i++) {
            try (PreparedStatement stmt = this.getPreparedStatement(conn, fillerYStmt)) {
                stmt.setInt(1, read_nonhotkeys.get(i));
                try (ResultSet r = stmt.executeQuery()) {
                    while (r.next()) {
                        for (int j = 0; j < YCSBConstants.NUM_FIELDS; j++) {
                            results[j] = r.getString(j + 1);
                        }
                    }
                }
            }
        }

        // Write to set of write keys
        for (int i = 0; i < write_nonhotkeys.size(); i++) {
            try (PreparedStatement stmt = this.getPreparedStatement(conn, updateZStmt)) {
                stmt.setInt(11, write_nonhotkeys.get(i));

                for (int j = 0; j < fields.length; j++) {
                    stmt.setString(j + 1, fields[j]);
                }
                stmt.executeUpdate();
            }
        }

        // // Commit trx
        // try (PreparedStatement stmt = this.getPreparedStatement(conn, commitStmt)) {
        //     stmt.execute();
        // }
    }

}
