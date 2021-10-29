/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.sdk;

import com._4paradigm.openmldb.*;
import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public interface SqlExecutor {
    boolean createDB(String db);

    boolean dropDB(String db);

    boolean executeDDL(String db, String sql);

    boolean executeInsert(String db, String sql);

    boolean executeInsert(String db, String sql, SQLInsertRow row);

    boolean executeInsert(String db, String sql, SQLInsertRows rows);
    TableReader getTableReader();
    java.sql.ResultSet executeSQL(String db, String sql);

    SQLInsertRow getInsertRow(String db, String sql);

    SQLInsertRows getInsertRows(String db, String sql);

    ResultSet executeSQLRequest(String db, String sql, SQLRequestRow row);

    PreparedStatement getInsertPreparedStmt(String db, String sql) throws SQLException;

    PreparedStatement getRequestPreparedStmt(String db, String sql) throws SQLException;
    PreparedStatement getPreparedStatement(String db, String sql) throws SQLException;

    PreparedStatement getBatchRequestPreparedStmt(String db, String sql,
                                                  List<Integer> commonColumnIndices) throws SQLException;

    CallablePreparedStatement getCallablePreparedStmt(String db, String spName) throws SQLException;

    CallablePreparedStatement getCallablePreparedStmtBatch(String db, String spName) throws SQLException;

    Schema getInputSchema(String dbName, String sql) throws SQLException;

    Schema getTableSchema(String dbName, String tableName) throws SQLException;

    ProcedureInfo showProcedure(String dbName, String proName) throws SQLException;

    List<CreateTableDesc> genDDL(String sql, List<DataBaseDesc> dataBases) throws SQLException;

    void close();
}
