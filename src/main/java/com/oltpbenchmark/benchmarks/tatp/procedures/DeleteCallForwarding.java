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


package com.oltpbenchmark.benchmarks.tatp.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tatp.TATPConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DeleteCallForwarding extends Procedure {

    public final SQLStmt stmtStartTrxForSQL = new SQLStmt(
        "START TRANSACTION ? FOR (?, ?)"
    );

    public final SQLStmt getSubscriberNew = new SQLStmt(
            "SELECT * FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE s_id = ?"
    );

    public final SQLStmt getSubscriber = new SQLStmt(
            "SELECT s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr = ?"
    );

    public final SQLStmt updateCallForwarding = new SQLStmt(
            "DELETE FROM " + TATPConstants.TABLENAME_CALL_FORWARDING +
                    " WHERE s_id = ? AND sf_type = ? AND start_time = ?"
    );

    public long run(Connection conn, long s_id, long sf_type, byte start_time) throws SQLException { // String sub_nbr,
        int type = (int) ((s_id - 1) * 4 + sf_type);
        if (s_id > 20) {
            type = 100;
        }
        // System.out.printf("USD s_id: %d sf_type: %d type%d%n", s_id, sf_type, type);
        try (PreparedStatement stmt = this.getPreparedStatement(conn, stmtStartTrxForSQL)) {
            stmt.setInt(1, type); // NewOrder trx type = 0
            stmt.setInt(2, 0);
            stmt.setInt(3, 7);
            stmt.execute();
        }

        // long s_id = -1;

        // try (PreparedStatement stmt = this.getPreparedStatement(conn, getSubscriber)) {
        //     stmt.setString(1, sub_nbr);
        //     try (ResultSet results = stmt.executeQuery()) {
        //         if (results.next()) {
        //             s_id = results.getLong(1);
        //         }
        //     }
        // }

        try (PreparedStatement stmt = this.getPreparedStatement(conn, getSubscriberNew)) {
            stmt.setLong(1, s_id);
            try (ResultSet results = stmt.executeQuery()) {
                while (results.next()) {
                    continue;
                }
            }
        }

        int rows_updated = -1;

        try (PreparedStatement stmt = this.getPreparedStatement(conn, updateCallForwarding)) {
            stmt.setLong(1, s_id);
            stmt.setByte(2, new Long(sf_type).byteValue());
            stmt.setByte(3, start_time);
            rows_updated = stmt.executeUpdate();
        }

        if (rows_updated == 0) {
            throw new UserAbortException("Failed to delete a row in " + TATPConstants.TABLENAME_CALL_FORWARDING);
        }
        return (rows_updated);
    }
}
