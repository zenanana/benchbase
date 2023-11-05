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

        Set<Integer> set = new HashSet<Integer>();
        // Bunch of filler stmts
        for (int i = 0; i < keys.size(); i++) {
            if (randInt(0,1) < 1) {
                try (PreparedStatement stmt = this.getPreparedStatement(conn, fillerYStmt)) {
                    stmt.setInt(1, keys.get(i));
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
                    stmt.setInt(11, keys.get(i));
                    // int randInt = randInt(YCSBConstants.RECORD_COUNT + 1, 500000);
                    // stmt.setInt(11, randInt);
                    for (int j = 0; j < fields.length; j++) {
                        stmt.setString(j + 1, fields[j]);
                    }
                    stmt.executeUpdate();
                }
            }
        }

    }

}
