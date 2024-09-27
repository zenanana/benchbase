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

public class UpdateSubscriberData extends Procedure {
    public final SQLStmt selectSubscriber = new SQLStmt(
            "SELECT * FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE s_id = ?"
    );

    public final SQLStmt selectSpecialFacility = new SQLStmt(
            "SELECT * FROM " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " WHERE s_id = ? AND sf_type = ?"
    );

    public final SQLStmt updateSubscriber = new SQLStmt(
            "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET bit_1 = ? WHERE s_id = ?"
    );

    public final SQLStmt updateSpecialFacility = new SQLStmt(
            "UPDATE " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " SET data_a = ? WHERE s_id = ? AND sf_type = ?"
    );

    public long run(Connection conn, long s_id, byte bit_1, short data_a, long sf_type) throws SQLException {
        int updated;

        try (PreparedStatement stmt = this.getPreparedStatement(conn, selectSubscriber)) {
            stmt.setLong(1, s_id);
            try (ResultSet results = stmt.executeQuery()) {

            }
        }

        try (PreparedStatement stmt = this.getPreparedStatement(conn, selectSpecialFacility)) {
            stmt.setLong(1, s_id);
            stmt.setByte(2, new Long(sf_type).byteValue());
            try (ResultSet results = stmt.executeQuery()) {

            }
        }

        try (PreparedStatement stmt = this.getPreparedStatement(conn, updateSubscriber)) {
            stmt.setByte(1, bit_1);
            stmt.setLong(2, s_id);
            updated = stmt.executeUpdate();
        }

        try (PreparedStatement stmt = this.getPreparedStatement(conn, updateSpecialFacility)) {
            stmt.setShort(1, data_a);
            stmt.setLong(2, s_id);
            stmt.setByte(3, new Long(sf_type).byteValue());
            updated = stmt.executeUpdate();
        }
        System.out.printf("USD s_id: %d sf_type: %d updated: %d%n", s_id, sf_type, updated);
        // if (updated != 0) {
        //     System.out.printf("ABORT USD s_id: %d sf_type: %d updated: %d%n", s_id, sf_type, updated);
        //     throw new UserAbortException("Failed to update a row in " + TATPConstants.TABLENAME_SPECIAL_FACILITY);
        // }
        return (updated);
    }
}

// public class UpdateSubscriberData extends Procedure {

//     public final SQLStmt stmtStartTrxForSQL = new SQLStmt(
//         "START TRANSACTION ? FOR (?, ?)"
//     );

//     public final SQLStmt updateSubscriber = new SQLStmt(
//             "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET bit_1 = ? WHERE s_id = ?"
//     );

//     public final SQLStmt updateSpecialFacility = new SQLStmt(
//             "UPDATE " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " SET data_a = ? WHERE s_id = ? AND sf_type = ?"
//     );

//     public long run(Connection conn, long s_id, byte bit_1, short data_a, long sf_type) throws SQLException {
//         // long s_id = 2;
//         // long sf_type = 1;
//         int type = (int) ((s_id - 1) * 4 + sf_type);
//         // if (s_id > 20) {
//         //     type = 100;
//         // }
//         // // System.out.printf("USD s_id: %d sf_type: %d type%d%n", s_id, sf_type, type);
//         // try (PreparedStatement stmt = this.getPreparedStatement(conn, stmtStartTrxForSQL)) {
//         //     stmt.setInt(1, type); // NewOrder trx type = 0
//         //     stmt.setInt(2, 0);
//         //     stmt.setInt(3, 7);
//         //     stmt.execute();
//         // }

//         int updated;

//         try (PreparedStatement stmt = this.getPreparedStatement(conn, updateSubscriber)) {
//             stmt.setByte(1, bit_1);
//             stmt.setLong(2, s_id);
//             updated = stmt.executeUpdate();
//         }

//         // try (PreparedStatement stmt = this.getPreparedStatement(conn, updateSpecialFacility)) {
//         //     stmt.setShort(1, data_a);
//         //     stmt.setLong(2, s_id);
//         //     stmt.setByte(3, new Long(sf_type).byteValue());
//         //     updated = stmt.executeUpdate();
//         // }
//         if (updated != 0) {
//             System.out.printf("ABORT USD s_id: %d sf_type: %d type%d%n", s_id, sf_type, type);
//             throw new UserAbortException("Failed to update a row in " + TATPConstants.TABLENAME_SPECIAL_FACILITY);
//         }
//         return (updated);
//     }
// }
