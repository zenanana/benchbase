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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


/*
Notation for X, Y, Z:
X: first set of hotkeys
Y: large filler set of keys
Z: second set of hotkeys
*/
public class ReadZWriteXRecord extends Procedure {
    /**
     * get random integer in range [min, max]
     */
    public int randInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    public final SQLStmt selectXStmt = new SQLStmt(
        "SELECT * FROM " + TABLE_NAME + " where YCSB_KEY=?" // FOR UPDATE
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
    public void run(Connection conn, int trx_typ, Integer[] trx_args, int X, int Z, int Z_start, int Z_end, String[] fields, String[] results) throws SQLException {
        // TODO: get Z_start and Z_end within function rather than passing as arguments

        // Start trx for stmt
        try (PreparedStatement stmt = this.getPreparedStatement(conn, startTrxForStmt)) {
            stmt.setInt(1, trx_typ);
            stmt.setInt(2, trx_args[0]);
            stmt.setInt(3, trx_args[1]);
            stmt.execute();
        }

        // Fetch it!
        try (PreparedStatement stmt = this.getPreparedStatement(conn, selectXStmt)) {
            stmt.setInt(1, Z);
            try (ResultSet r = stmt.executeQuery()) {
                while (r.next()) {
                    for (int i = 0; i < YCSBConstants.NUM_FIELDS; i++) {
                        results[i] = r.getString(i + 1);
                    }
                }
            }
        }

        try (PreparedStatement stmt = this.getPreparedStatement(conn, updateZStmt)) {
            stmt.setInt(11, Z);

            for (int i = 0; i < fields.length; i++) {
                stmt.setString(i + 1, fields[i]);
            }
            stmt.executeUpdate();
        }

        // Bunch of filler stmts
        for (int i = 0; i < YCSBConstants.FILLER_STMT_SIZE; i++) {
            try (PreparedStatement stmt = this.getPreparedStatement(conn, fillerYStmt)) {
                stmt.setInt(1, randInt(Z_start, Z_end));
                try (ResultSet r = stmt.executeQuery()) {
                    while (r.next()) {
                        for (int j = 0; j < YCSBConstants.NUM_FIELDS; j++) {
                            results[j] = r.getString(j + 1);
                        }
                    }
                }
            }
        }

        try (PreparedStatement stmt = this.getPreparedStatement(conn, selectXStmt)) {
            stmt.setInt(1, X);
            try (ResultSet r = stmt.executeQuery()) {
                while (r.next()) {
                    for (int i = 0; i < YCSBConstants.NUM_FIELDS; i++) {
                        results[i] = r.getString(i + 1);
                    }
                }
            }
        }

        // Update that mofo
        try (PreparedStatement stmt = this.getPreparedStatement(conn, updateZStmt)) {
            stmt.setInt(11, X);

            for (int i = 0; i < fields.length; i++) {
                stmt.setString(i + 1, fields[i]);
            }
            stmt.executeUpdate();
        }

        // // Commit stmt
        // try (PreparedStatement stmt = this.getPreparedStatement(conn, commitStmt)) {
        //     stmt.execute();
        // }
    }

}
