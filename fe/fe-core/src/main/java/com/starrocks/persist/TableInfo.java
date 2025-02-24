// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

public class TableInfo implements Writable {

    @SerializedName("db")
    private long dbId;
    @SerializedName("tb")
    private long tableId;
    @SerializedName("idx")
    private long indexId;
    @SerializedName("pt")
    private long partitionId;

    @SerializedName("nt")
    private String newTableName;
    @SerializedName("nr")
    private String newRollupName;
    @SerializedName("np")
    private String newPartitionName;

    public TableInfo() {
        // for persist
    }

    private TableInfo(long dbId, long tableId, long indexId, long partitionId,
                      String newTableName, String newRollupName, String newPartitionName) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.indexId = indexId;
        this.partitionId = partitionId;

        this.newTableName = newTableName;
        this.newRollupName = newRollupName;
        this.newPartitionName = newPartitionName;
    }

    public static TableInfo createForTableRename(long dbId, long tableId, String newTableName) {
        return new TableInfo(dbId, tableId, -1L, -1L, newTableName, "", "");
    }

    public static TableInfo createForRollupRename(long dbId, long tableId, long indexId, String newRollupName) {
        return new TableInfo(dbId, tableId, indexId, -1L, "", newRollupName, "");
    }

    public static TableInfo createForPartitionRename(long dbId, long tableId, long partitionId,
                                                     String newPartitionName) {
        return new TableInfo(dbId, tableId, -1L, partitionId, "", "", newPartitionName);
    }

    public static TableInfo createForModifyDistribution(long dbId, long tableId) {
        return new TableInfo(dbId, tableId, -1L, -1, "", "", "");
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public String getNewTableName() {
        return newTableName;
    }

    public String getNewRollupName() {
        return newRollupName;
    }

    public String getNewPartitionName() {
        return newPartitionName;
    }


}
