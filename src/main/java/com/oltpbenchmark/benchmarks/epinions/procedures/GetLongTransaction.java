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

package com.oltpbenchmark.benchmarks.epinions.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Collections;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class GetLongTransaction extends Procedure {
    /**
     * get random integer in range [min, max]
     */
    public int randInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    public final SQLStmt stmtStartTrxForSQL = new SQLStmt(
            "START TRANSACTION ? FOR (?, ?)"
    );

    public final SQLStmt getItem = new SQLStmt(
            "SELECT * FROM item i WHERE i.i_id = ? "
    );

    public final SQLStmt getItemForUpdate = new SQLStmt(
            "SELECT * FROM item i WHERE i.i_id = ? FOR UPDATE"
    );

    public final SQLStmt getAverageRating = new SQLStmt(
            "SELECT avg(rating) FROM review r WHERE r.i_id=?"
    );

    public final SQLStmt updateItem = new SQLStmt(
            "UPDATE item SET title = ? WHERE i_id = ? "
    );

    public void run(Connection conn, ArrayList<Integer> keys, String title, int schedule) throws SQLException {

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

        int[] read_keys = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        int[] write_keys = {0};

        int type = randInt(0,7);
        // boolean finalWrite = false;
        // if (randInt(0,99) < 35) {
        //     finalWrite = true;
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

            // if (randInt(0,99) < 1) {
            //     type = randInt(0,7);
            // }
        // } else {
        //     type = 11;
        // }

        // System.out.printf("type %d read_keys %s write_keys %s%n", type,
        //     Arrays.toString(read_keys), Arrays.toString(write_keys));

        try (PreparedStatement stmt = this.getPreparedStatement(conn, stmtStartTrxForSQL)) {
            if (schedule != 0) {
                stmt.setInt(1, type);
            } else {
                stmt.setInt(1, 0);
            }
            stmt.setInt(2, 0);
            stmt.setInt(3, 7);
            stmt.execute();
        }


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
                try (PreparedStatement stmt = this.getPreparedStatement(conn, getItem)) {
                    stmt.setLong(1, Long.valueOf(read_keys[i]));
                    try (ResultSet r = stmt.executeQuery()) {
                        while (r.next()) {
                            continue;
                        }
                    }
                }
            }
        }

        for (int i = 0; i < keys.size(); i++) {
            try (PreparedStatement stmt = this.getPreparedStatement(conn, getAverageRating)) {
                stmt.setLong(1, Long.valueOf(keys.get(i)));
                try (ResultSet r = stmt.executeQuery()) {
                    while (r.next()) {
                        continue;
                    }
                }
            }
        }

        for (int i = 0; i < write_keys.length; i++) {
            try (PreparedStatement stmt = this.getPreparedStatement(conn, getItemForUpdate)) {
                stmt.setLong(1, Long.valueOf(write_keys[i]));
                try (ResultSet r = stmt.executeQuery()) {
                    while (r.next()) {
                        continue;
                    }
                }
            }

            try (PreparedStatement stmt = this.getPreparedStatement(conn, updateItem)) {
                stmt.setString(1, title);
                stmt.setLong(2, Long.valueOf(write_keys[i]));
                stmt.executeUpdate();
            }
        }
    }
}
