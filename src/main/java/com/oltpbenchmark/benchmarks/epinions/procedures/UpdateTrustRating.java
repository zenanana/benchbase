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

public class UpdateTrustRating extends Procedure {

    public final SQLStmt stmtStartTrxForSQL = new SQLStmt(
        "START TRANSACTION ? FOR (?, ?)"
    );

    public final SQLStmt selectTrust = new SQLStmt(
            "SELECT * FROM trust WHERE source_u_id=? AND target_u_id=?"
    );

    public final SQLStmt updateTrust = new SQLStmt(
            "UPDATE trust SET trust = ? WHERE source_u_id=? AND target_u_id=?"
    );

    public void run(Connection conn, long source_uid, long target_uid, int trust) throws SQLException {
        // int type = (int) Math.min(source_uid, target_uid);
        // if (type > 10) {
        //     type = 21;
        // }
        // // System.out.printf("NO w_id: %d d_id: %d type%d%n", w_id, d_id, type);
        // try (PreparedStatement stmt = this.getPreparedStatement(conn, stmtStartTrxForSQL)) {
        //     stmt.setInt(1, type); // NewOrder trx type = 0
        //     stmt.setInt(2, 0);
        //     stmt.setInt(3, 7);
        //     stmt.execute();
        // }

        try (PreparedStatement stmt = this.getPreparedStatement(conn, selectTrust)) {
            stmt.setLong(1, source_uid);
            stmt.setLong(2, target_uid);
            try (ResultSet r = stmt.executeQuery()) {
                while (r.next()) {
                    continue;
                }
            }
        }

        try (PreparedStatement stmt = this.getPreparedStatement(conn, updateTrust)) {
            stmt.setInt(1, trust);
            stmt.setLong(2, source_uid);
            stmt.setLong(3, target_uid);
            stmt.executeUpdate();
        }
    }
}
