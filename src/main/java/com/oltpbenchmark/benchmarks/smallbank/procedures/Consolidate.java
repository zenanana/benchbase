/***************************************************************************
 *  Copyright (C) 2013 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.smallbank.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Consolidate Procedure
 */
public class Consolidate extends Procedure {
    /**
     * get random integer in range [min, max]
     */
    public int randInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    // 2013-05-05
    // In the original version of the benchmark, this is suppose to be a look up
    // on the customer's name. We don't have fast implementation of replicated
    // secondary indexes, so we'll just ignore that part for now.
    public final SQLStmt GetAccount = new SQLStmt(
            "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
                    " WHERE custid = ?"
    );

    public final SQLStmt stmtStartTrxForSQL = new SQLStmt(
            "START TRANSACTION ? FOR (?, ?)"
    );

    public final SQLStmt GetSavingsBalance = new SQLStmt(
            "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
                    " WHERE custid = ?"
    );

    public final SQLStmt GetCheckingBalance = new SQLStmt(
            "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
                    " WHERE custid = ?"
    );

    public final SQLStmt GetCheckingBalanceUpdate = new SQLStmt(
            "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
                    " WHERE custid = ? FOR UPDATE"
    );


    public final SQLStmt UpdateSavingsBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS +
                    "   SET bal = bal - ? " +
                    " WHERE custid = ?"
    );

    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_CHECKING +
                    "   SET bal = bal + ? " +
                    " WHERE custid = ?"
    );

    public final SQLStmt ZeroCheckingBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_CHECKING +
                    "   SET bal = bal + 0.0 " +
                    " WHERE custid = ?"
    );

    public void run(Connection conn, long custId0, long custId1, int schedule) throws SQLException {

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
                stmt.setInt(1,  type+1); //type+101);//101); //
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
                // Get Account Information
                try (PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, Long.valueOf(read_keys[i]))) {
                    try (ResultSet r0 = stmt0.executeQuery()) {
                        if (!r0.next()) {
                            String msg = "Invalid account '" + Long.valueOf(read_keys[i]) + "'";
                            throw new UserAbortException(msg);
                        }
                    }
                }
                try (PreparedStatement balStmt0 = this.getPreparedStatement(conn, GetSavingsBalance, Long.valueOf(read_keys[i]))) {
                    try (ResultSet balRes0 = balStmt0.executeQuery()) {
                        if (!balRes0.next()) {
                            String msg = String.format("No %s for customer #%d",
                                    SmallBankConstants.TABLENAME_SAVINGS,
                                    Long.valueOf(read_keys[i]));
                            throw new UserAbortException(msg);
                        }
                    }
                }

                double checkingBalance;
                try (PreparedStatement balStmt1 = this.getPreparedStatement(conn, GetCheckingBalance, Long.valueOf(read_keys[i]))) {
                    try (ResultSet balRes1 = balStmt1.executeQuery()) {
                        if (!balRes1.next()) {
                            String msg = String.format("No %s for customer #%d",
                                    SmallBankConstants.TABLENAME_CHECKING,
                                    Long.valueOf(read_keys[i]));
                            throw new UserAbortException(msg);
                        }
                    }
                }
            }
        }



        try (PreparedStatement stmt1 = this.getPreparedStatement(conn, GetAccount, Long.valueOf(write_keys[0]))) {
            try (ResultSet r1 = stmt1.executeQuery()) {
                if (!r1.next()) {
                    String msg = "Invalid account '" + Long.valueOf(write_keys[0]) + "'";
                    throw new UserAbortException(msg);
                }
            }
        }

        // Get Balance Information
        double savingsBalance;
        try (PreparedStatement balStmt0 = this.getPreparedStatement(conn, GetSavingsBalance, custId0)) {
            try (ResultSet balRes0 = balStmt0.executeQuery()) {
                if (!balRes0.next()) {
                    String msg = String.format("No %s for customer #%d",
                            SmallBankConstants.TABLENAME_SAVINGS,
                            custId0);
                    throw new UserAbortException(msg);
                }
                savingsBalance = balRes0.getDouble(1);
            }
        }

        // double checkingBalance;
        // try (PreparedStatement balStmt1 = this.getPreparedStatement(conn, GetCheckingBalance, custId1)) {
        //     try (ResultSet balRes1 = balStmt1.executeQuery()) {
        //         if (!balRes1.next()) {
        //             String msg = String.format("No %s for customer #%d",
        //                     SmallBankConstants.TABLENAME_CHECKING,
        //                     custId1);
        //             throw new UserAbortException(msg);
        //         }

        //         checkingBalance = balRes1.getDouble(1);
        //     }
        // }

        try (PreparedStatement balStmt1 = this.getPreparedStatement(conn, GetCheckingBalanceUpdate, Long.valueOf(write_keys[0]))) {
            try (ResultSet balRes1 = balStmt1.executeQuery()) {
                if (!balRes1.next()) {
                    String msg = String.format("No %s for customer #%d",
                            SmallBankConstants.TABLENAME_CHECKING,
                            Long.valueOf(write_keys[0]));
                    throw new UserAbortException(msg);
                }
            }
        }

        // double total = checkingBalance + savingsBalance;
        // assert(total >= 0);

        // Update Balance Information
        int status;
        try (PreparedStatement updateStmt0 = this.getPreparedStatement(conn, ZeroCheckingBalance, Long.valueOf(write_keys[0]))) {
            status = updateStmt0.executeUpdate();
        }


        // try (PreparedStatement updateStmt1 = this.getPreparedStatement(conn, UpdateSavingsBalance, total, custId1)) {
        //     status = updateStmt1.executeUpdate();
        // }

    }
}
