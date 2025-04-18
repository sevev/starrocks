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

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

/**
 * used for batch modify multi partitions in one atomic operation
 */

public class BatchModifyPartitionsInfo implements Writable {
    @SerializedName(value = "infos")
    private List<ModifyPartitionInfo> infos;

    public BatchModifyPartitionsInfo(List<ModifyPartitionInfo> infos) {
        this.infos = infos;
    }



    public static BatchModifyPartitionsInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, BatchModifyPartitionsInfo.class);
    }

    public List<ModifyPartitionInfo> getModifyPartitionInfos() {
        return infos;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(infos);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BatchModifyPartitionsInfo)) {
            return false;
        }
        List<ModifyPartitionInfo> otherInfos = ((BatchModifyPartitionsInfo) other).getModifyPartitionInfos();
        for (ModifyPartitionInfo thisInfo : infos) {
            for (ModifyPartitionInfo otherInfo : otherInfos) {
                if (!thisInfo.equals(otherInfo)) {
                    return false;
                }
            }
        }
        return true;
    }
}
