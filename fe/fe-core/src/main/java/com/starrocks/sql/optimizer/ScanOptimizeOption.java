// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer;

public class ScanOptimizeOption {
    private boolean canUseAnyColumn;
    private boolean canUseMinMaxOpt;
    private boolean usePartitionColumnValueOnly;
    private boolean canUseCountOpt;

    public void setCanUseAnyColumn(boolean v) {
        canUseAnyColumn = v;
    }

    public void setCanUseMinMaxOpt(boolean v) {
        canUseMinMaxOpt = v;
    }

    public boolean getCanUseAnyColumn() {
        return canUseAnyColumn;
    }

    public boolean getCanUseMinMaxOpt() {
        return canUseMinMaxOpt;
    }

    public void setUsePartitionColumnValueOnly(boolean v) {
        this.usePartitionColumnValueOnly = v;
    }

    public boolean getUsePartitionColumnValueOnly() {
        return usePartitionColumnValueOnly;
    }

    public void setCanUseCountOpt(boolean v) {
        this.canUseCountOpt = v;
    }

    public boolean getCanUseCountOpt() {
        return canUseCountOpt;
    }

    public ScanOptimizeOption copy() {
        ScanOptimizeOption opt = new ScanOptimizeOption();
        opt.canUseAnyColumn = this.canUseAnyColumn;
        opt.canUseMinMaxOpt = this.canUseMinMaxOpt;
        opt.usePartitionColumnValueOnly = this.usePartitionColumnValueOnly;
        opt.canUseCountOpt = this.canUseCountOpt;
        return opt;
    }
}
