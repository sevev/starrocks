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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletScanKeyConstraint;
import com.starrocks.thrift.TTabletScanKeyConstraintType;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers {@link OlapScanNode#updateScanRangeLocations}, the export path that re-resolves scan ranges
 * against the current materialized index. It builds the tablet-scan-key constraint from scratch
 * rather than copying the one in the incoming range, so it needs its own coverage: a bucket ordinal
 * carried over from the original plan could describe a stale tablet order.
 */
public class OlapScanNodeScanKeyConstraintTest {

    private static final long INDEX_META_ID = 10L;
    private static final long PARTITION_ID = 100L;
    private static final long PHYSICAL_PARTITION_ID = 101L;
    private static final long BACKEND_ID = 10001L;
    private static final int SCHEMA_HASH = 0;
    private static final long VISIBLE_VERSION = 1L;
    private static final long[] TABLET_IDS = {1001L, 1002L, 1003L, 1004L};

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static Column intKey(String name) {
        Column column = new Column(name, IntegerType.INT);
        column.setIsKey(true);
        return column;
    }

    private static OlapTable tableWith(List<Column> schema, List<Column> distributionColumns) {
        OlapTable olapTable = new OlapTable(Table.TableType.OLAP);
        olapTable.maySetDatabaseId(1L);
        olapTable.setBaseIndexMetaId(INDEX_META_ID);
        olapTable.setIndexMeta(INDEX_META_ID, "base", schema, 0, SCHEMA_HASH, (short) schema.size(),
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setDefaultDistributionInfo(
                new HashDistributionInfo(TABLET_IDS.length, distributionColumns));

        MaterializedIndex index = new MaterializedIndex(INDEX_META_ID);
        // Both ids must be the meta id: PhysicalPartition#getLatestIndex looks the index up by meta id.
        index.setIdForRestore(INDEX_META_ID);
        for (long tabletId : TABLET_IDS) {
            LocalTablet tablet = new LocalTablet(tabletId);
            tablet.addReplica(new Replica(tabletId, BACKEND_ID, VISIBLE_VERSION, SCHEMA_HASH,
                    1024L, 10L, Replica.ReplicaState.NORMAL, -1L, VISIBLE_VERSION));
            index.addTablet(tablet, null, false);
        }
        olapTable.addPartition(new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p0", index,
                olapTable.getDefaultDistributionInfo()));
        return olapTable;
    }

    private static OlapScanNode scanNodeFor(OlapTable olapTable) {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(olapTable);
        OlapScanNode node = new OlapScanNode(new PlanNodeId(0), desc, "OlapScanNode", INDEX_META_ID);
        node.setSelectedPartitionIds(Lists.newArrayList(PARTITION_ID));
        return node;
    }

    private static List<TScanRangeLocations> incomingRanges() {
        List<TScanRangeLocations> locations = Lists.newArrayList();
        for (long tabletId : TABLET_IDS) {
            TInternalScanRange internalScanRange = new TInternalScanRange();
            internalScanRange.setTablet_id(tabletId);
            internalScanRange.setPartition_id(PHYSICAL_PARTITION_ID);
            internalScanRange.setSchema_hash(String.valueOf(SCHEMA_HASH));
            internalScanRange.setVersion(String.valueOf(VISIBLE_VERSION));

            TScanRange scanRange = new TScanRange();
            scanRange.setInternal_scan_range(internalScanRange);
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackend_id(BACKEND_ID);

            TScanRangeLocations one = new TScanRangeLocations();
            one.setScan_range(scanRange);
            one.setLocations(Lists.newArrayList(location));
            locations.add(one);
        }
        return locations;
    }

    private static ConnectContext contextWithPrune(boolean enabled) {
        ConnectContext ctx = new ConnectContext();
        ctx.setQueryId(UUIDUtil.genUUID());
        // GlobalStateMgr is mocked here, so the context's own session variable would be a mock whose
        // setters do nothing. Install a real one.
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableTabletScanKeyPrune(enabled);
        ctx.setSessionVariable(sessionVariable);
        ctx.setThreadLocalInfo();
        return ctx;
    }

    private void expectAliveBackend(GlobalStateMgr globalStateMgr, NodeMgr nodeMgr,
                                    SystemInfoService systemInfoService) {
        Backend backend = new Backend(BACKEND_ID, "127.0.0.1", 9050);
        backend.setBePort(9060);
        backend.setAlive(true);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;
                nodeMgr.getClusterInfo();
                result = systemInfoService;
                minTimes = 0;
                systemInfoService.getBackendOrComputeNode(anyLong);
                result = backend;
                minTimes = 0;
            }
        };
    }

    @Test
    public void attachesConstraintWithOrdinalFromCurrentIndex(@Mocked GlobalStateMgr globalStateMgr,
                                                              @Mocked NodeMgr nodeMgr,
                                                              @Mocked SystemInfoService systemInfoService)
            throws Exception {
        expectAliveBackend(globalStateMgr, nodeMgr, systemInfoService);
        Column k1 = intKey("k1");
        OlapTable olapTable = tableWith(Lists.newArrayList(k1), Lists.newArrayList(k1));
        OlapScanNode node = scanNodeFor(olapTable);
        contextWithPrune(true);

        List<TScanRangeLocations> out = node.updateScanRangeLocations(incomingRanges(),
                WarehouseManager.DEFAULT_RESOURCE);

        assertEquals(TABLET_IDS.length, out.size());
        for (int ordinal = 0; ordinal < out.size(); ordinal++) {
            TInternalScanRange range = out.get(ordinal).scan_range.internal_scan_range;
            assertTrue(range.isSetScan_key_constraint(),
                    "tablet " + range.getTablet_id() + " carries no constraint");
            TTabletScanKeyConstraint constraint = range.getScan_key_constraint();
            assertNotNull(constraint);
            assertEquals(TTabletScanKeyConstraintType.HASH_BUCKET, constraint.getType());
            // Ordinal and bucket count both come from the index this scan now targets.
            assertEquals(ordinal, constraint.getBucket_id());
            assertEquals(TABLET_IDS.length, constraint.getBucket_num());
            assertEquals(Lists.newArrayList(0), constraint.getDistribution_key_positions());
            // The export scan covers every tablet, so no tablet was selected by a hash match and an
            // empty prune result must not be read as a broken hash contract.
            assertFalse(constraint.isPruning_was_exact());
        }
    }

    @Test
    public void sendsNothingWhenPruneDisabled(@Mocked GlobalStateMgr globalStateMgr,
                                              @Mocked NodeMgr nodeMgr,
                                              @Mocked SystemInfoService systemInfoService)
            throws Exception {
        expectAliveBackend(globalStateMgr, nodeMgr, systemInfoService);
        Column k1 = intKey("k1");
        OlapTable olapTable = tableWith(Lists.newArrayList(k1), Lists.newArrayList(k1));
        OlapScanNode node = scanNodeFor(olapTable);
        contextWithPrune(false);

        List<TScanRangeLocations> out = node.updateScanRangeLocations(incomingRanges(),
                WarehouseManager.DEFAULT_RESOURCE);

        assertEquals(TABLET_IDS.length, out.size());
        for (TScanRangeLocations one : out) {
            assertFalse(one.scan_range.internal_scan_range.isSetScan_key_constraint());
        }
    }

    @Test
    public void sendsNothingWhenDistributionColumnIsNotInSortKey(@Mocked GlobalStateMgr globalStateMgr,
                                                                 @Mocked NodeMgr nodeMgr,
                                                                 @Mocked SystemInfoService systemInfoService)
            throws Exception {
        expectAliveBackend(globalStateMgr, nodeMgr, systemInfoService);
        Column k1 = intKey("k1");
        Column v1 = new Column("v1", IntegerType.INT);
        // Distribution on a value column: no scan key can ever carry it, so the whole scan bails out.
        OlapTable olapTable = tableWith(Lists.newArrayList(k1, v1), Lists.newArrayList(v1));
        OlapScanNode node = scanNodeFor(olapTable);
        contextWithPrune(true);

        List<TScanRangeLocations> out = node.updateScanRangeLocations(incomingRanges(),
                WarehouseManager.DEFAULT_RESOURCE);

        assertEquals(TABLET_IDS.length, out.size());
        for (TScanRangeLocations one : out) {
            assertFalse(one.scan_range.internal_scan_range.isSetScan_key_constraint());
        }
    }
}
