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

package com.starrocks.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedView.PartitionRefreshStrategy;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockParams;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.HivePartitionDataInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.TableUpdateArbitrator;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.mv.MVPCTMetaRepairer;
import com.starrocks.scheduler.mv.MVPCTRefreshListPartitioner;
import com.starrocks.scheduler.mv.MVPCTRefreshNonPartitioner;
import com.starrocks.scheduler.mv.MVPCTRefreshPartitioner;
import com.starrocks.scheduler.mv.MVPCTRefreshPlanBuilder;
import com.starrocks.scheduler.mv.MVPCTRefreshRangePartitioner;
import com.starrocks.scheduler.mv.MVRefreshParams;
import com.starrocks.scheduler.mv.MVTraceUtils;
import com.starrocks.scheduler.mv.MVVersionManager;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.ListPartitionDiffer;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;
import static com.starrocks.scheduler.TaskRun.MV_ID;
import static com.starrocks.scheduler.TaskRun.MV_UNCOPYABLE_PROPERTIES;

/**
 * Core logic of mv refresh task run
 * PartitionBasedMvRefreshProcessor is not thread safe for concurrent runs of the same materialized view
 */
public class PartitionBasedMvRefreshProcessor extends BaseTaskRunProcessor {
    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);
    // session.enable_spill
    private static final String MV_SESSION_ENABLE_SPILL =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.ENABLE_SPILL;
    // session.query_timeout. Deprecated, only for compatibility with old version
    private static final String MV_SESSION_QUERY_TIMEOUT =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.QUERY_TIMEOUT;
    // session.insert_timeout
    private static final String MV_SESSION_INSERT_TIMEOUT =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.INSERT_TIMEOUT;

    // Set of table types that support adaptive materialized view (MV) refresh.
    //
    // Internal tables (e.g., OLAP) have direct access to partition information,
    // enabling accurate and efficient adaptive MV refresh.
    //
    // For external tables (e.g., Hive, Iceberg, Hudi, Delta Lake),
    // partition information is not directly available.
    // Instead, adaptive refresh relies on statistics collected in the
    // `external_column_statistics` table under the `_statistics_` database,
    // which currently only supports these external table types (since v3.3.0).
    private static final Set<Table.TableType> SUPPORTED_TABLE_TYPES_FOR_ADAPTIVE_MV_REFRESH = EnumSet.of(
            Table.TableType.OLAP,
            Table.TableType.HIVE,
            Table.TableType.ICEBERG,
            Table.TableType.HUDI,
            Table.TableType.DELTALAKE
    );

    private Database db;
    private MaterializedView mv;
    private Logger logger;
    private MvTaskRunContext mvContext;
    // only trigger to post process when mv has been refreshed successfully
    private boolean isNeedToTriggerPostProcess = false;

    // Collect all bases tables of the mv to be updated meta after mv refresh success.
    // format :     table id -> <base table info, snapshot table>
    private Map<Long, TableSnapshotInfo> snapshotBaseTables = Maps.newHashMap();

    private long oldTransactionVisibleWaitTimeout;

    @VisibleForTesting
    private RuntimeProfile runtimeProfile;
    private MVPCTRefreshPartitioner mvRefreshPartitioner;
    // for testing
    private TaskRun nextTaskRun = null;

    public PartitionBasedMvRefreshProcessor() {
    }

    @VisibleForTesting
    public MvTaskRunContext getMvContext() {
        return mvContext;
    }

    @VisibleForTesting
    public void setMvContext(MvTaskRunContext mvContext) {
        this.mvContext = mvContext;
    }

    @VisibleForTesting
    public RuntimeProfile getRuntimeProfile() {
        return runtimeProfile;
    }

    public TaskRun getNextTaskRun() {
        return nextTaskRun;
    }

    // Core logics:
    // 1. prepare to check some conditions
    // 2. sync partitions with base tables(add or drop partitions, which will be optimized  by dynamic partition creation later)
    // 3. decide which partitions of mv to refresh and the corresponding base tables' source partitions
    // 4. construct the refresh sql and execute it
    // 5. update the source table version map if refresh task completes successfully
    @Override
    public Constants.TaskRunState processTaskRun(TaskRunContext context) throws Exception {
        // register tracers
        Tracers.register(context.getCtx());
        QueryDebugOptions queryDebugOptions = context.getCtx().getSessionVariable().getQueryDebugOptions();
        // init to collect the base timer for refresh profile
        Tracers.Mode mvRefreshTraceMode = queryDebugOptions.getMvRefreshTraceMode();
        Tracers.Module mvRefreshTraceModule = queryDebugOptions.getMvRefreshTraceModule();
        Tracers.init(mvRefreshTraceMode, mvRefreshTraceModule, true, false);

        IMaterializedViewMetricsEntity mvEntity = null;
        ConnectContext connectContext = context.getCtx();
        QueryMaterializationContext queryMVContext = new QueryMaterializationContext();
        connectContext.setQueryMVContext(queryMVContext);
        try {
            // do refresh
            try (Timer ignored = Tracers.watchScope("MVRefreshDoWholeRefresh")) {
                // refresh mv
                Preconditions.checkState(mv != null);
                mvEntity = MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
                Constants.TaskRunState taskRunState = doMvRefresh(context, mvEntity);
                // update metrics
                mvEntity.increaseRefreshJobStatus(taskRunState);
                connectContext.getState().setOk();
                // only trigger to post process when mv has been refreshed successfully
                this.isNeedToTriggerPostProcess = taskRunState == Constants.TaskRunState.SUCCESS;
                return taskRunState;
            }
        } catch (Exception e) {
            if (mvEntity != null) {
                mvEntity.increaseRefreshJobStatus(Constants.TaskRunState.FAILED);
            }
            connectContext.getState().setError(e.getMessage());
            throw e;
        } finally {
            try {
                // reset query mv context to avoid affecting other tasks
                queryMVContext.clear();
                connectContext.setQueryMVContext(null);

                if (FeConstants.runningUnitTest) {
                    runtimeProfile = new RuntimeProfile();
                    Tracers.toRuntimeProfile(runtimeProfile);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("refresh mv trace logs: {}", Tracers.getTrace(mvRefreshTraceMode));
                }
                Tracers.close();
                postProcess();
            } catch (Exception e) {
                logger.error("Failed to close Tracers", e);
            }
        }
    }

    /**
     * Sync partitions of base tables and check whether they are changing anymore
     */
    private boolean syncAndCheckPartitions(TaskRunContext context,
                                           IMaterializedViewMetricsEntity mvEntity,
                                           Map<TableSnapshotInfo, Set<String>> baseTableCandidatePartitions)
            throws AnalysisException, LockTimeoutException {
        // collect partition infos of ref base tables
        int retryNum = 0;
        boolean checked = false;
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (!checked && retryNum++ < Config.max_mv_check_base_table_change_retry_times) {
            mvEntity.increaseRefreshRetryMetaCount(1L);
            try (Timer ignored = Tracers.watchScope("MVRefreshExternalTable")) {
                // refresh external table meta cache before sync partitions
                refreshExternalTable(context, baseTableCandidatePartitions);
            }

            if (!Config.enable_materialized_view_external_table_precise_refresh || retryNum > 1) {
                try (Timer ignored = Tracers.watchScope("MVRefreshSyncPartitions")) {
                    // sync partitions between mv and base tables out of lock
                    // do it outside lock because it is a time-cost operation
                    if (!syncPartitions()) {
                        logger.warn("Sync partitions failed.");
                        return false;
                    }
                }
            }

            try (Timer ignored = Tracers.watchScope("MVRefreshCheckBaseTableChange")) {
                // check whether there are partition changes for base tables, eg: partition rename
                // retry to sync partitions if any base table changed the partition infos
                if (checkBaseTablePartitionChange(mv)) {
                    logger.info("materialized view base partition has changed. retry to sync partitions, retryNum:{}", retryNum);
                    // sleep 100ms
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    continue;
                }
            }
            checked = true;
        }
        Tracers.record("MVRefreshSyncPartitionsRetryTimes", String.valueOf(retryNum));
        logger.info("sync and check mv partition changing after {} times: {}, costs: {} ms",
                retryNum, checked, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return checked;
    }

    /**
     * Compute the partitioned to be refreshed in this task, according to [start, end), ttl, and other context info
     * If it's tentative, only return the result rather than modify any state
     * IF it's not, it would modify the context state, like `NEXT_PARTITION_START`
     */
    @VisibleForTesting
    public Set<String> checkMvToRefreshedPartitions(TaskRunContext context, boolean tentative)
            throws AnalysisException, LockTimeoutException {
        Set<String> mvToRefreshedPartitions = null;
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(db.getId(), mv.getId(),
                LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            logger.warn("failed to lock database: {} in checkMvToRefreshedPartitions", db.getFullName());
            throw new LockTimeoutException("Failed to lock database: " + db.getFullName());
        }
        PartitionRefreshStrategy partitionRefreshStrategy = PartitionRefreshStrategy.valueOf(
                mv.getTableProperty().getPartitionRefreshStrategy().trim().toUpperCase());

        boolean isForce = partitionRefreshStrategy == PartitionRefreshStrategy.FORCE || tentative;
        try {
            final Set<String> mvPotentialPartitionNames = Sets.newHashSet();
            final MVRefreshParams mvRefreshParams = new MVRefreshParams(mv.getPartitionInfo(), context.getProperties(), isForce);
            final PartitionInfo partitionInfo = mv.getPartitionInfo();
            mvToRefreshedPartitions = getPartitionsToRefreshForMaterializedView(partitionInfo,
                    mvRefreshParams, mvPotentialPartitionNames);
            // update mv extra message
            if (!tentative) {
                updateTaskRunStatus(status -> {
                    MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
                    extraMessage.setForceRefresh(mvRefreshParams.isForce());
                    extraMessage.setPartitionStart(mvRefreshParams.getRangeStart());
                    extraMessage.setPartitionEnd(mvRefreshParams.getRangeEnd());
                });
            }
            if (CollectionUtils.isEmpty(mvToRefreshedPartitions)) {
                logger.info("no partitions to refresh for materialized view");
                return mvToRefreshedPartitions;
            }

            boolean hasUnsupportedTableType = mv.getBaseTableTypes().stream()
                    .anyMatch(type -> !SUPPORTED_TABLE_TYPES_FOR_ADAPTIVE_MV_REFRESH.contains(type));
            if (hasUnsupportedTableType) {
                logger.warn("Materialized view {} contains unsupported external tables. Using default refresh strategy.",
                        mv.getId());
                filterPartitionByRefreshNumber(mvToRefreshedPartitions, mvPotentialPartitionNames, mv,
                        tentative);
            } else {
                switch (partitionRefreshStrategy) {
                    case ADAPTIVE:
                        filterPartitionByAdaptiveRefreshNumber(mvToRefreshedPartitions, mvPotentialPartitionNames, mv,
                                tentative);
                        break;
                    case STRICT:
                    default:
                        // Only refresh the first partition refresh number partitions, other partitions will generate new tasks
                        filterPartitionByRefreshNumber(mvToRefreshedPartitions, mvPotentialPartitionNames, mv,
                                tentative);
                }
            }

            int partitionRefreshNumber = mv.getTableProperty().getPartitionRefreshNumber();
            logger.info("filter partitions to refresh partitionRefreshNumber={}, partitionsToRefresh:{}, " +
                            "mvPotentialPartitionNames:{}, next start:{}, next end:{}, next list values:{}",
                    partitionRefreshNumber, mvToRefreshedPartitions, mvPotentialPartitionNames,
                    mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd(), mvContext.getNextPartitionValues());
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), mv.getId(), LockType.READ);
        }
        return mvToRefreshedPartitions;
    }

    private Constants.TaskRunState doMvRefresh(TaskRunContext context, IMaterializedViewMetricsEntity mvEntity) {
        long startRefreshTs = System.currentTimeMillis();

        // log mv basic info, it may throw exception if mv is invalid since base table has dropped
        try {
            logger.debug("refBaseTablePartitionExprMap:{},refBaseTablePartitionSlotMap:{}, " +
                            "refBaseTablePartitionColumnMap:{},baseTableInfos:{}",
                    mv.getRefBaseTablePartitionExprs(), mv.getRefBaseTablePartitionSlots(),
                    mv.getRefBaseTablePartitionColumns(), MvUtils.formatBaseTableInfos(mv.getBaseTableInfos()));
        } catch (Throwable e) {
            logger.warn("Log mv basic info failed:", e);
        }

        // refresh materialized view
        Constants.TaskRunState result = doRefreshMaterializedViewWithRetry(context, mvEntity);

        // do not generate next task run if the current task run is killed
        if (mvContext.hasNextBatchPartition() && !mvContext.getTaskRun().isKilled()) {
            generateNextTaskRun();
        }

        long refreshDurationMs = System.currentTimeMillis() - startRefreshTs;
        logger.info("refresh mv success, cost time(ms): {}", DebugUtil.DECIMAL_FORMAT_SCALE_3.format(refreshDurationMs));
        mvEntity.updateRefreshDuration(refreshDurationMs);
        return result;
    }

    private void logMvToRefreshInfoIntoTaskRun(Set<String> finalMvToRefreshedPartitions,
                                               Map<String, Set<String>> finalRefTablePartitionNames) {
        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
            extraMessage.setMvPartitionsToRefresh(finalMvToRefreshedPartitions);
            extraMessage.setRefBasePartitionsToRefreshMap(finalRefTablePartitionNames);
        });
    }

    /**
     * Retry the `doRefreshMaterializedView` method to avoid insert fails in occasional cases.
     */
    private Constants.TaskRunState doRefreshMaterializedViewWithRetry(
            TaskRunContext taskRunContext,
            IMaterializedViewMetricsEntity mvEntity) throws DmlException {
        // Use current connection variables instead of mvContext's session variables to be better debug.
        int maxRefreshMaterializedViewRetryNum = getMaxRefreshMaterializedViewRetryNum(taskRunContext.getCtx());
        logger.info("start to refresh mv with retry times:{}", maxRefreshMaterializedViewRetryNum);

        Throwable lastException = null;
        int lockFailedTimes = 0;
        int refreshFailedTimes = 0;
        while (refreshFailedTimes < maxRefreshMaterializedViewRetryNum &&
                lockFailedTimes < Config.max_mv_refresh_try_lock_failure_retry_times) {
            try {
                if (refreshFailedTimes > 0) {
                    UUID uuid = UUID.randomUUID();
                    ConnectContext context = taskRunContext.getCtx();
                    logger.info("transfer QueryId: {} to {}", DebugUtil.printId(context.getQueryId()),
                            DebugUtil.printId(uuid));
                    context.setExecutionId(
                            new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
                }
                Tracers.record("MVRefreshRetryTimes", String.valueOf(refreshFailedTimes));
                Tracers.record("MVRefreshLockRetryTimes", String.valueOf(lockFailedTimes));
                return doRefreshMaterializedView(taskRunContext, mvEntity);
            } catch (LockTimeoutException e) {
                // if lock timeout, retry to refresh
                lockFailedTimes += 1;
                logger.warn("refresh mv failed at {}th time because try lock failed: {}",
                        lockFailedTimes, DebugUtil.getStackTrace(e));
                lastException = e;
            } catch (Throwable e) {
                refreshFailedTimes += 1;
                logger.warn("refresh mv failed at {}th time: {}", refreshFailedTimes, DebugUtil.getRootStackTrace(e));
                lastException = e;
            }

            // sleep some time if it is not the last retry time
            Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
        }

        // throw the last exception if all retries failed
        String errorMsg = MvUtils.shrinkToSize(DebugUtil.getRootStackTrace(lastException), MAX_FIELD_VARCHAR_LENGTH);
        throw new DmlException("Refresh mv %s failed after %s times, try lock failed: %s, error-msg : " +
                "%s", lastException, mv.getName(), refreshFailedTimes, lockFailedTimes, errorMsg);
    }

    private static int getMaxRefreshMaterializedViewRetryNum(ConnectContext currConnectCtx) {
        int maxRefreshMaterializedViewRetryNum = 1;
        if (currConnectCtx != null && currConnectCtx.getSessionVariable() != null) {
            maxRefreshMaterializedViewRetryNum =
                    currConnectCtx.getSessionVariable().getQueryDebugOptions().getMaxRefreshMaterializedViewRetryNum();
            if (maxRefreshMaterializedViewRetryNum <= 0) {
                maxRefreshMaterializedViewRetryNum = 1;
            }
        }
        maxRefreshMaterializedViewRetryNum = Math.max(Config.max_mv_refresh_failure_retry_times,
                maxRefreshMaterializedViewRetryNum);
        return maxRefreshMaterializedViewRetryNum;
    }

    private Constants.TaskRunState doRefreshMaterializedView(TaskRunContext context,
                                                             IMaterializedViewMetricsEntity mvEntity) throws Exception {
        // 0. Compute the base-table partitions to check for external table
        // The candidate partition info is used to refresh the external table
        Map<TableSnapshotInfo, Set<String>> baseTableCandidatePartitions = Maps.newHashMap();
        if (Config.enable_materialized_view_external_table_precise_refresh) {
            try (Timer ignored = Tracers.watchScope("MVRefreshComputeCandidatePartitions")) {
                if (!syncPartitions()) {
                    throw new DmlException(String.format("materialized view %s refresh task failed: sync partition failed",
                            mv.getName()));
                }
                Set<String> mvCandidatePartition = checkMvToRefreshedPartitions(context, true);
                baseTableCandidatePartitions = getRefTableRefreshPartitions(mvCandidatePartition);
            } catch (Exception e) {
                logger.warn("failed to compute candidate partitions in sync partitions", DebugUtil.getRootStackTrace(e));
                // Since at here we sync partitions before the refreshExternalTable, the situation may happen that
                // the base-table not exists before refreshExternalTable, so we just need to swallow this exception
                if (e.getMessage() == null || !e.getMessage().contains("not exist")) {
                    throw e;
                }
            }
        }

        // 1. Refresh the partition information of these base-table partitions, and create mv partitions if needed
        try (Timer ignored = Tracers.watchScope("MVRefreshSyncAndCheckPartitions")) {
            if (!syncAndCheckPartitions(context, mvEntity, baseTableCandidatePartitions)) {
                throw new DmlException(String.format("materialized view %s refresh task failed: sync partition failed",
                        mv.getName()));
            }
        }

        Set<String> mvToRefreshedPartitions = null;
        Map<TableSnapshotInfo, Set<String>> refTableRefreshPartitions = null;
        Map<String, Set<String>> refTablePartitionNames = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshCheckMVToRefreshPartitions")) {
            mvToRefreshedPartitions = checkMvToRefreshedPartitions(context, false);
            if (CollectionUtils.isEmpty(mvToRefreshedPartitions)) {
                return Constants.TaskRunState.SKIPPED;
            }
            // ref table of mv : refreshed partition names
            refTableRefreshPartitions = getRefTableRefreshPartitions(mvToRefreshedPartitions);
            // ref table of mv : refreshed partition names
            refTablePartitionNames = refTableRefreshPartitions.entrySet().stream()
                    .collect(Collectors.toMap(x -> x.getKey().getName(), Map.Entry::getValue));
            logger.info("mvToRefreshedPartitions:{}, refTableRefreshPartitions:{}",
                    mvToRefreshedPartitions, refTableRefreshPartitions);
            // add a message into information_schema
            logMvToRefreshInfoIntoTaskRun(mvToRefreshedPartitions, refTablePartitionNames);
            updateBaseTablePartitionSnapshotInfos(refTableRefreshPartitions);
        }

        ///// 2. execute the ExecPlan of insert stmt
        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshPrepareRefreshPlan")) {
            insertStmt = prepareRefreshPlan(mvToRefreshedPartitions, refTablePartitionNames);
        }
        try (Timer ignored = Tracers.watchScope("MVRefreshMaterializedView")) {
            refreshMaterializedView(mvContext, mvContext.getExecPlan(), insertStmt);
        }

        ///// 3. insert execute successfully, update the meta of mv according to ExecPlan
        try (Timer ignored = Tracers.watchScope("MVRefreshUpdateMeta")) {
            updateMeta(mvToRefreshedPartitions, mvContext.getExecPlan(), refTableRefreshPartitions);
        }

        return Constants.TaskRunState.SUCCESS;
    }

    /**
     * Prepare the statement and plan for mv refreshing, considering the partitions of ref table
     */
    private InsertStmt prepareRefreshPlan(Set<String> mvToRefreshedPartitions, Map<String, Set<String>> refTablePartitionNames)
            throws AnalysisException, LockTimeoutException {
        long startTime = System.currentTimeMillis();
        // 1. Prepare context
        ConnectContext ctx = mvContext.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(mvContext.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase())
                .setWarehouse(ctx.getCurrentWarehouseName())
                .setCNGroup(ctx.getCurrentComputeResourceName());

        // 2. Prepare variables
        changeDefaultConnectContextIfNeeded(ctx);

        // 3. AST
        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshParser")) {
            insertStmt = generateInsertAst(mvToRefreshedPartitions, mv, ctx);
        }

        PlannerMetaLocker locker = new PlannerMetaLocker(ctx, insertStmt);
        ExecPlan execPlan = null;
        if (!locker.tryLock(Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database in prepareRefreshPlan");
        }

        MVPCTRefreshPlanBuilder planBuilder = new MVPCTRefreshPlanBuilder(db, mv, mvContext, mvRefreshPartitioner);
        try {
            // 4. Analyze and prepare a partition & Rebuild insert statement by
            // considering to-refresh partitions of ref tables/ mv
            try (Timer ignored = Tracers.watchScope("MVRefreshAnalyzer")) {
                insertStmt = planBuilder.analyzeAndBuildInsertPlan(insertStmt,
                        mvToRefreshedPartitions, refTablePartitionNames, ctx);
                // Must set execution id before StatementPlanner.plan
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
            }

            // 5. generate insert stmt's exec plan, make thread local ctx existed
            try (ConnectContext.ScopeGuard guard = ctx.bindScope(); Timer ignored = Tracers.watchScope("MVRefreshPlanner")) {
                execPlan = StatementPlanner.planInsertStmt(locker, insertStmt, ctx);
            }
        } finally {
            locker.unlock();
        }

        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage message = status.getMvTaskRunExtraMessage();
            if (message == null) {
                return;
            }
            Map<String, String> planBuildMessage = planBuilder.getPlanBuilderMessage();
            logger.info("MV Refresh PlanBuilderMessage: {}", planBuildMessage);
            message.setPlanBuilderMessage(planBuildMessage);
        });

        QueryDebugOptions debugOptions = ctx.getSessionVariable().getQueryDebugOptions();
        // log the final mv refresh plan for each refresh for better trace and debug
        if (logger.isDebugEnabled() || debugOptions.isEnableQueryTraceLog()) {
            logger.info("MV Refresh Final Plan\nMV PartitionsToRefresh: {}\nBase PartitionsToScan: {}\n" +
                            "Insert Plan:\n{}",
                    String.join(",", mvToRefreshedPartitions), refTablePartitionNames,
                    execPlan != null ? execPlan.getExplainString(StatementBase.ExplainLevel.VERBOSE) : "");
        } else {
            logger.info("MV Refresh Final Plan, MV PartitionsToRefresh: {}, Base PartitionsToScan: {}",
                    String.join(",", mvToRefreshedPartitions), refTablePartitionNames);
        }
        mvContext.setExecPlan(execPlan);
        return insertStmt;
    }

    /**
     * Change default connect context when for mv refresh this is because:
     * - MV Refresh may take much resource to load base tables' data into the final materialized view.
     * - Those changes are set by default and also able to be changed by users for their needs.
     *
     * @param mvConnectCtx
     */
    private void changeDefaultConnectContextIfNeeded(ConnectContext mvConnectCtx) {
        // add resource group if resource group is enabled
        TableProperty mvProperty = mv.getTableProperty();
        SessionVariable mvSessionVariable = mvConnectCtx.getSessionVariable();
        if (mvSessionVariable.isEnableResourceGroup()) {
            String rg = ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME;
            if (mvProperty != null && !Strings.isNullOrEmpty(mvProperty.getResourceGroup())) {
                rg = mvProperty.getResourceGroup();
            }
            mvSessionVariable.setResourceGroup(rg);
        }

        // enable spill by default for mv if spill is not set by default and
        // `session.enable_spill` session variable is not set.
        if (Config.enable_materialized_view_spill &&
                !mvSessionVariable.isEnableSpill() &&
                !mvProperty.getProperties().containsKey(MV_SESSION_ENABLE_SPILL)) {
            mvSessionVariable.setEnableSpill(true);
        }

        if (!mvProperty.getProperties().containsKey(MV_SESSION_INSERT_TIMEOUT)
                && mvProperty.getProperties().containsKey(MV_SESSION_QUERY_TIMEOUT)) {
            // for compatibility
            mvProperty.getProperties().put(MV_SESSION_INSERT_TIMEOUT, mvProperty.getProperties().get(MV_SESSION_QUERY_TIMEOUT));
        }

        // set insert_max_filter_ratio by default
        if (!isMVPropertyContains(SessionVariable.INSERT_MAX_FILTER_RATIO)) {
            mvSessionVariable.setInsertMaxFilterRatio(Config.mv_refresh_fail_on_filter_data ? 0 : 1);
        }
        // enable profile by default for mv refresh task
        if (!isMVPropertyContains(SessionVariable.ENABLE_PROFILE) && !mvSessionVariable.isEnableProfile()) {
            mvSessionVariable.setEnableProfile(Config.enable_mv_refresh_collect_profile);
        }
        // set the default new_planner_optimize_timeout for mv refresh
        if (!isMVPropertyContains(SessionVariable.NEW_PLANNER_OPTIMIZER_TIMEOUT)) {
            mvSessionVariable.setOptimizerExecuteTimeout(Config.mv_refresh_default_planner_optimize_timeout);
        }
        // set enable_materialized_view_rewrite by default
        if (!isMVPropertyContains(SessionVariable.ENABLE_MATERIALIZED_VIEW_REWRITE) && Config.enable_mv_refresh_query_rewrite) {
            // Only enable mv rewrite when there are more than one related mvs that can be rewritten by other mvs.
            Set<Table> baseTables = snapshotBaseTables.values().stream()
                    .map(t -> t.getBaseTable()).collect(Collectors.toSet());
            if (isEnableMVRefreshQueryRewrite(mvConnectCtx, baseTables)) {
                mvSessionVariable.setEnableMaterializedViewRewrite(Config.enable_mv_refresh_query_rewrite);
                mvSessionVariable.setEnableMaterializedViewRewriteForInsert(Config.enable_mv_refresh_query_rewrite);
            }
        }
        // set nested_mv_rewrite_max_level by default, only rewrite one level
        if (!isMVPropertyContains(SessionVariable.NESTED_MV_REWRITE_MAX_LEVEL)) {
            mvSessionVariable.setNestedMvRewriteMaxLevel(1);
        }
        // always exclude the current mv name from rewrite
        mvSessionVariable.setQueryExcludingMVNames(mv.getName());
        mvConnectCtx.setUseConnectorMetadataCache(Optional.of(true));
    }

    private boolean isMVPropertyContains(String key) {
        String mvKey = PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + key;
        return mv.getTableProperty().getProperties().containsKey(mvKey);
    }

    private void postProcess() {
        // If mv's not active, mvContext may be null.
        if (mvContext != null && mvContext.ctx != null) {
            mvContext.ctx.getSessionVariable().setTransactionVisibleWaitTimeout(oldTransactionVisibleWaitTimeout);
        }
    }

    private boolean isEnableMVRefreshQueryRewrite(ConnectContext ctx,
                                                  Set<Table> baseTables) {
        return MvUtils.getRelatedMvs(ctx, 1, baseTables).size() > 1;
    }

    public MVTaskRunExtraMessage getMVTaskRunExtraMessage() {
        if (this.mvContext.status == null) {
            return null;
        }
        return this.mvContext.status.getMvTaskRunExtraMessage();
    }

    @VisibleForTesting
    public void filterPartitionByRefreshNumber(Set<String> partitionsToRefresh,
                                               Set<String> mvPotentialPartitionNames,
                                               MaterializedView mv) {
        filterPartitionByRefreshNumber(partitionsToRefresh, mvPotentialPartitionNames, mv, false);
    }

    public void filterPartitionByRefreshNumber(Set<String> partitionsToRefresh,
                                               Set<String> mvPotentialPartitionNames,
                                               MaterializedView mv,
                                               boolean tentative) {
        // refresh all partition when it's a sync refresh, otherwise updated partitions may be lost.
        if (mvContext.executeOption != null && mvContext.executeOption.getIsSync()) {
            return;
        }
        // ignore if mv is not partitioned.
        if (!mv.isPartitionedTable()) {
            return;
        }
        // ignore if partition_fresh_limit is not set
        int partitionRefreshNumber = mv.getTableProperty().getPartitionRefreshNumber();
        if (partitionRefreshNumber <= 0) {
            return;
        }

        // do filter actions
        mvRefreshPartitioner.filterPartitionByRefreshNumber(partitionsToRefresh, mvPotentialPartitionNames, tentative);
    }

    @VisibleForTesting
    public void filterPartitionByAdaptiveRefreshNumber(Set<String> partitionsToRefresh,
                                                       Set<String> mvPotentialPartitionNames,
                                                       MaterializedView mv) {
        filterPartitionByAdaptiveRefreshNumber(partitionsToRefresh, mvPotentialPartitionNames, mv, false);
    }

    public void filterPartitionByAdaptiveRefreshNumber(Set<String> partitionsToRefresh,
                                                       Set<String> mvPotentialPartitionNames,
                                                       MaterializedView mv,
                                                       boolean tentative) {
        // refresh all partition when it's a sync refresh, otherwise updated partitions may be lost.
        if (mvContext.executeOption != null && mvContext.executeOption.getIsSync()) {
            return;
        }
        // ignore if mv is not partitioned.
        if (!mv.isPartitionedTable()) {
            return;
        }

        // do filter actions
        mvRefreshPartitioner.filterPartitionByAdaptiveRefreshNumber(partitionsToRefresh, mvPotentialPartitionNames, tentative);
    }

    private void generateNextTaskRun() {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Map<String, String> properties = mvContext.getProperties();
        long mvId = Long.parseLong(properties.get(MV_ID));
        String taskName = TaskBuilder.getMvTaskName(mvId);
        Map<String, String> newProperties = Maps.newHashMap();
        for (Map.Entry<String, String> proEntry : properties.entrySet()) {
            // skip uncopyable properties: force/partition_values/... which only can be set specifically.
            if (proEntry.getKey() == null || proEntry.getValue() == null
                    || MV_UNCOPYABLE_PROPERTIES.contains(proEntry.getKey())) {
                continue;
            }
            newProperties.put(proEntry.getKey(), proEntry.getValue());
        }
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo.isListPartition()) {
            //TODO: partition values may be too long, need to be optimized later.
            newProperties.put(TaskRun.PARTITION_VALUES, mvContext.getNextPartitionValues());
        } else {
            newProperties.put(TaskRun.PARTITION_START, mvContext.getNextPartitionStart());
            newProperties.put(TaskRun.PARTITION_END, mvContext.getNextPartitionEnd());
        }
        if (mvContext.getStatus() != null) {
            newProperties.put(TaskRun.START_TASK_RUN_ID, mvContext.getStatus().getStartTaskRunId());
        }
        // warehouse
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            newProperties.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE));
        }
        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
            extraMessage.setNextPartitionStart(mvContext.getNextPartitionStart());
            extraMessage.setNextPartitionEnd(mvContext.getNextPartitionEnd());
            extraMessage.setNextPartitionValues(mvContext.getNextPartitionValues());
        });

        // Partition refreshing task run should have the HIGHER priority, and be scheduled before other tasks
        // Otherwise this round of partition refreshing would be staved and never got finished
        int priority = mvContext.executeOption.getPriority() > Constants.TaskRunPriority.LOWEST.value() ?
                mvContext.executeOption.getPriority() : Constants.TaskRunPriority.HIGHER.value();
        ExecuteOption option = new ExecuteOption(priority, true, newProperties);
        logger.info("[MV] Generate a task to refresh next batches of partitions for MV {}-{}, start={}, end={}, " +
                        "priority={}, properties={}", mv.getName(), mv.getId(),
                mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd(), priority, properties);

        if (properties.containsKey(TaskRun.IS_TEST) && properties.get(TaskRun.IS_TEST).equalsIgnoreCase("true")) {
            // for testing
            TaskRun taskRun = TaskRunBuilder
                    .newBuilder(taskManager.getTask(taskName))
                    .properties(option.getTaskRunProperties())
                    .setExecuteOption(option)
                    .build();
            nextTaskRun = taskRun;
        } else {
            taskManager.executeTask(taskName, option);
        }
    }

    private void refreshExternalTable(TaskRunContext context,
                                      Map<TableSnapshotInfo, Set<String>> baseTableCandidatePartitions) {
        List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        // use it if refresh external table fails
        ConnectContext connectContext = context.getCtx();
        List<Pair<Table, BaseTableInfo>> toRepairTables = new ArrayList<>();
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Optional<Database> dbOpt =
                    GlobalStateMgr.getCurrentState().getMetadataMgr().getDatabase(connectContext, baseTableInfo);
            if (dbOpt.isEmpty()) {
                logger.warn("database {} do not exist in refreshing materialized view", baseTableInfo.getDbInfoStr());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }

            Optional<Table> optTable = MvUtils.getTable(baseTableInfo);
            if (optTable.isEmpty()) {
                logger.warn("table {} do not exist when refreshing materialized view", baseTableInfo.getTableInfoStr());
                mv.setInactiveAndReason(
                        MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(baseTableInfo.getTableName()));
                throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
            }

            // refresh old table
            Table table = optTable.get();
            // if table is native table or materialized view or connector view or external table, no need to refresh
            if (table.isNativeTableOrMaterializedView() || table.isView()
                    || MaterializedViewAnalyzer.isExternalTableFromResource(table)) {
                logger.debug("No need to refresh table:{} because it is native table or mv or connector view",
                        baseTableInfo.getTableInfoStr());
                continue;
            }
            TableSnapshotInfo snapshotInfo = new TableSnapshotInfo(baseTableInfo, table);
            Set<String> basePartitions = baseTableCandidatePartitions.get(snapshotInfo);
            if (CollectionUtils.isNotEmpty(basePartitions)) {
                // only refresh referenced partitions, to reduce metadata overhead
                List<String> realPartitionNames = basePartitions.stream()
                        .flatMap(name -> mvContext.getExternalTableRealPartitionName(table, name).stream())
                        .collect(Collectors.toList());
                connectContext.getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), table, realPartitionNames, false);
            } else {
                // refresh the whole table, which may be costly in extreme case
                connectContext.getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), table, Lists.newArrayList(), true);
            }
            // should clear query cache
            connectContext.getGlobalStateMgr().getMetadataMgr().removeQueryMetadata();

            // check new table
            Optional<Table> optNewTable = MvUtils.getTable(baseTableInfo);
            if (optNewTable.isEmpty()) {
                logger.warn("table {} does not exist after refreshing materialized view", baseTableInfo.getTableInfoStr());
                mv.setInactiveAndReason(
                        MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(baseTableInfo.getTableName()));
                throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
            }

            Table newTable = optNewTable.get();
            // only collect to-repair tables when the table is not the same as the old one by checking the table identifier
            if (!baseTableInfo.getTableIdentifier().equals(table.getTableIdentifier())) {
                toRepairTables.add(Pair.create(newTable, baseTableInfo));
            }
        }

        // do repair if needed
        if (!toRepairTables.isEmpty()) {
            MVPCTMetaRepairer.repairMetaIfNeeded(db, mv, toRepairTables);
        }
    }

    /**
     * After mv is refreshed, update materialized view's meta info to record history refreshes.
     *
     * @param refTableAndPartitionNames : refreshed base table and its partition names mapping.
     */
    private void updateMeta(Set<String> mvRefreshedPartitions,
                            ExecPlan execPlan,
                            Map<TableSnapshotInfo, Set<String>> refTableAndPartitionNames) {
        // check
        Table mv = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), this.mv.getId());
        if (mv == null) {
            throw new DmlException("update meta failed. materialized view:" + this.mv.getName() + " not exist");
        }
        // check
        if (mvRefreshedPartitions == null || refTableAndPartitionNames == null) {
            logger.info("no partitions to refresh, mvRefreshedPartitions:{}, refTableAndPartitionNames:{}",
                    mvRefreshedPartitions, refTableAndPartitionNames);
            return;
        }

        // update mv's version info
        Set<Long> refBaseTableIds = refTableAndPartitionNames.keySet().stream()
                .map(t -> t.getId())
                .collect(Collectors.toSet());

        Locker locker = new Locker();
        // update the meta if succeed
        if (!locker.lockDatabaseAndCheckExist(db, this.mv, LockType.WRITE)) {
            logger.warn("failed to lock database: {} in updateMeta for mv refresh", db.getFullName());
            throw new DmlException("update meta failed. database:" + db.getFullName() + " not exist");
        }

        MVVersionManager mvVersionManager = new MVVersionManager(this.mv, mvContext);
        try {
            mvVersionManager.updateMVVersionInfo(snapshotBaseTables, mvRefreshedPartitions,
                    refBaseTableIds, refTableAndPartitionNames);
        } catch (Exception e) {
            logger.warn("update final meta failed after mv refreshed:", DebugUtil.getRootStackTrace(e));
            throw e;
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), this.mv.getId(), LockType.WRITE);
        }

        // update mv status message
        updateTaskRunStatus(status -> {
            try {
                MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
                Map<String, Set<String>> baseTableRefreshedPartitionsByExecPlan =
                        MVTraceUtils.getBaseTableRefreshedPartitionsByExecPlan(this.mv, execPlan);
                extraMessage.setBasePartitionsToRefreshMap(baseTableRefreshedPartitionsByExecPlan);
            } catch (Exception e) {
                // just log warn and no throw exceptions for an updating task runs message.
                logger.warn("update task run messages failed:", DebugUtil.getRootStackTrace(e));
            }
        });
    }

    @VisibleForTesting
    @Override
    public void prepare(TaskRunContext context) throws Exception {
        Map<String, String> properties = context.getProperties();
        // NOTE: mvId is set in Task's properties when creating
        long mvId = Long.parseLong(properties.get(MV_ID));
        this.db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(context.ctx.getDatabase());
        if (db == null) {
            throw new DmlException("database " + context.ctx.getDatabase() + " do not exist.");
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), mvId);
        if (table == null || !(table instanceof MaterializedView)) {
            throw new DmlException(String.format("materialized view:%s in database:%s do not exist when refreshing",
                    mvId, context.ctx.getDatabase()));
        }
        this.mv = (MaterializedView) table;
        // reinit the logger
        this.logger = MVTraceUtils.getLogger(mv, PartitionBasedMvRefreshProcessor.class);

        // try to activate the mv before refresh
        if (!mv.isActive()) {
            MVActiveChecker.tryToActivate(mv);
            logger.info("Activated the MV before refreshing: {}", mv.getName());
        }

        IMaterializedViewMetricsEntity mvEntity =
                MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
        if (!mv.isActive()) {
            String errorMsg = String.format("Materialized view: %s/%d is not active due to %s.",
                    mv.getName(), mvId, mv.getInactiveReason());
            logger.warn(errorMsg);
            mvEntity.increaseRefreshJobStatus(Constants.TaskRunState.FAILED);
            throw new DmlException(errorMsg);
        }
        // wait util transaction is visible for mv refresh task
        // because mv will update base tables' visible version after insert, the mv's visible version
        // should keep up with the base tables, or it will return outdated result.
        oldTransactionVisibleWaitTimeout = context.ctx.getSessionVariable().getTransactionVisibleWaitTimeout();
        context.ctx.getSessionVariable().setTransactionVisibleWaitTimeout(Long.MAX_VALUE / 1000);

        // Initialize status's job id which is used to track a batch of task runs.
        String jobId = properties.containsKey(TaskRun.START_TASK_RUN_ID) ?
                properties.get(TaskRun.START_TASK_RUN_ID) : context.getTaskRunId();
        if (context.status != null) {
            context.status.setStartTaskRunId(jobId);
        }

        // prepare mv context
        mvContext = new MvTaskRunContext(context);
        // prepare partition ttl number
        int partitionTTLNumber = mv.getTableProperty().getPartitionTTLNumber();
        mvContext.setPartitionTTLNumber(partitionTTLNumber);
        // prepare mv refresh partitioner
        this.mvRefreshPartitioner = buildMvRefreshPartitioner(mv, context);

        logger.info("finish prepare refresh mv:{}, jobId: {}", mvId, jobId);
    }

    /**
     * Create a mv refresh partitioner by the mv's partition info.
     */
    private MVPCTRefreshPartitioner buildMvRefreshPartitioner(MaterializedView mv, TaskRunContext context) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo.isUnPartitioned()) {
            return new MVPCTRefreshNonPartitioner(mvContext, context, db, mv);
        } else if (partitionInfo.isRangePartition()) {
            return new MVPCTRefreshRangePartitioner(mvContext, context, db, mv);
        } else if (partitionInfo.isListPartition()) {
            return new MVPCTRefreshListPartitioner(mvContext, context, db, mv);
        } else {
            throw new DmlException(String.format("materialized view:%s in database:%s refresh failed: partition info %s not " +
                    "supported", mv.getName(), context.ctx.getDatabase(), partitionInfo));
        }
    }

    /**
     * Sync base table's partition infos to be used later.
     */
    private boolean syncPartitions() throws AnalysisException, LockTimeoutException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        // collect base table snapshot infos
        snapshotBaseTables = collectBaseTableSnapshotInfos(mv);

        // do sync partitions (add or drop partitions) for materialized view
        boolean result = mvRefreshPartitioner.syncAddOrDropPartitions();
        logger.info("finish sync partitions, cost(ms): {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return result;
    }

    /**
     * Update task run status's extra message to add more information for information_schema if possible.
     *
     * @param action
     */
    private void updateTaskRunStatus(Consumer<TaskRunStatus> action) {
        if (this.mvContext.status != null) {
            action.accept(this.mvContext.status);
        }
    }

    /**
     * @param mvPartitionInfo : materialized view's partition info
     * @param mvRefreshParams : materialized view's refresh params
     * @return the partitions to refresh for materialized view
     * @throws AnalysisException
     */
    private Set<String> getPartitionsToRefreshForMaterializedView(PartitionInfo mvPartitionInfo,
                                                                  MVRefreshParams mvRefreshParams,
                                                                  Set<String> mvPotentialPartitionNames)
            throws AnalysisException {
        if (mvRefreshParams.isForceCompleteRefresh()) {
            // Force refresh
            return mvRefreshPartitioner.getMVPartitionsToRefreshWithForce();
        } else {
            return mvRefreshPartitioner.getMVPartitionsToRefresh(mvPartitionInfo, snapshotBaseTables,
                    mvRefreshParams, mvPotentialPartitionNames);
        }
    }

    /**
     * Build an AST for insert stmt
     */
    private InsertStmt generateInsertAst(Set<String> materializedViewPartitions, MaterializedView mv, ConnectContext ctx) {
        String definition = mvContext.getDefinition();
        InsertStmt insertStmt =
                (InsertStmt) SqlParser.parse(definition, ctx.getSessionVariable()).get(0);
        // set target partitions
        insertStmt.setTargetPartitionNames(new PartitionNames(false, new ArrayList<>(materializedViewPartitions)));
        // insert overwrite mv must set system = true
        insertStmt.setSystem(true);
        // if mv has set sort keys, materialized view's output columns
        // may be different from the defined query's output.
        // so set materialized view's defined outputs as target columns.
        List<Integer> queryOutputIndexes = mv.getQueryOutputIndices();
        List<Column> baseSchema = mv.getBaseSchemaWithoutGeneratedColumn();
        if (queryOutputIndexes != null && baseSchema.size() == queryOutputIndexes.size()) {
            List<String> targetColumnNames = queryOutputIndexes.stream()
                    .map(baseSchema::get)
                    .map(Column::getName)
                    .map(String::toLowerCase) // case insensitive
                    .collect(Collectors.toList());
            insertStmt.setTargetColumnNames(targetColumnNames);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("generate insert-overwrite statement, materialized view's target partition names:{}, " +
                            "mv's target columns: {}, definition:{}",
                    Joiner.on(",").join(materializedViewPartitions),
                    insertStmt.getTargetColumnNames() == null ? "" : Joiner.on(",").join(insertStmt.getTargetColumnNames()),
                    definition);
        }
        return insertStmt;
    }

    private boolean checkBaseTablePartitionHasChanged(TableSnapshotInfo snapshotInfo) {
        try {
            BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
            Table snapshotTable = snapshotInfo.getBaseTable();

            Optional<Table> optTable = MvUtils.getTableWithIdentifier(baseTableInfo);
            if (optTable.isEmpty()) {
                return true;
            }
            Table table = optTable.get();
            if (snapshotTable.isOlapOrCloudNativeTable()) {
                OlapTable snapShotOlapTable = (OlapTable) snapshotTable;
                PartitionInfo snapshotPartitionInfo = snapShotOlapTable.getPartitionInfo();
                if (snapshotPartitionInfo.isUnPartitioned()) {
                    Set<String> partitionNames = ((OlapTable) table).getVisiblePartitionNames();
                    if (!snapShotOlapTable.getVisiblePartitionNames().equals(partitionNames)) {
                        // there is partition rename
                        return true;
                    }
                } else if (snapshotPartitionInfo.isListPartition()) {
                    OlapTable snapshotOlapTable = (OlapTable) snapshotTable;
                    Map<String, PListCell> snapshotPartitionMap = snapshotOlapTable.getListPartitionItems();
                    Map<String, PListCell> currentPartitionMap = snapshotOlapTable.getListPartitionItems();
                    if (ListPartitionDiffer.hasListPartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
                        return true;
                    }
                } else {
                    Map<String, Range<PartitionKey>> snapshotPartitionMap = snapShotOlapTable.getRangePartitionMap();
                    Map<String, Range<PartitionKey>> currentPartitionMap = ((OlapTable) table).getRangePartitionMap();
                    if (SyncPartitionUtils.hasRangePartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
                        return true;
                    }
                }
            } else if (ConnectorPartitionTraits.isSupported(snapshotTable.getType())) {
                if (snapshotTable.isUnPartitioned()) {
                    return false;
                } else {
                    PartitionInfo mvPartitionInfo = mv.getPartitionInfo();
                    // TODO: Support list partition later.
                    // do not need to check base partition table changed when mv is not partitioned
                    if (!(mvPartitionInfo.isRangePartition())) {
                        return false;
                    }
                    Map<Table, List<Column>> partitionTableAndColumns = mv.getRefBaseTablePartitionColumns();
                    // For Non-partition based base table, it's not necessary to check the partition changed.
                    if (!partitionTableAndColumns.containsKey(snapshotTable)) {
                        return false;
                    }
                    List<Column> partitionColumns = partitionTableAndColumns.get(snapshotTable);
                    Preconditions.checkArgument(partitionColumns.size() == 1,
                            "Only support one partition column in range partition");
                    Column partitionColumn = partitionColumns.get(0);
                    Optional<Expr> rangePartitionExprOpt = mv.getRangePartitionFirstExpr();
                    if (rangePartitionExprOpt.isEmpty()) {
                        return false;
                    }
                    Expr rangePartitionExpr = rangePartitionExprOpt.get();
                    Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.getPartitionKeyRange(
                            snapshotTable, partitionColumn, rangePartitionExpr);
                    Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.getPartitionKeyRange(
                            table, partitionColumn, rangePartitionExpr);
                    if (SyncPartitionUtils.hasRangePartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
                        return true;
                    }
                }
            }
        } catch (StarRocksException e) {
            logger.warn("Materialized view compute partition change failed", DebugUtil.getRootStackTrace(e));
            return true;
        }
        return false;
    }

    /**
     * Check whether the base table's partition has changed or not. Wait to refresh until all mv's base tables
     * don't change again.
     *
     * @return: true if the base table's partition has changed, otherwise false.
     */
    private boolean checkBaseTablePartitionChange(MaterializedView mv) throws LockTimeoutException {
        LockParams lockParams = collectDatabases(mv);
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(lockParams,
                LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            logger.warn("failed to lock database: {} in checkBaseTablePartitionChange", lockParams);
            throw new LockTimeoutException("Failed to lock database: " + lockParams
                    + " in checkBaseTablePartitionChange");
        }
        // check snapshotBaseTables and current tables in catalog
        try {
            return snapshotBaseTables.values().stream().anyMatch(this::checkBaseTablePartitionHasChanged);
        } finally {
            locker.unLockTableWithIntensiveDbLock(lockParams, LockType.READ);
        }
    }

    @VisibleForTesting
    public void refreshMaterializedView(MvTaskRunContext mvContext, ExecPlan execPlan, InsertStmt insertStmt)
            throws Exception {
        Preconditions.checkNotNull(execPlan);
        Preconditions.checkNotNull(insertStmt);
        ConnectContext ctx = mvContext.getCtx();

        if (mvContext.getTaskRun().isKilled()) {
            logger.warn("[QueryId:{}] refresh materialized view {} is killed", ctx.getQueryId(),
                    mv.getName());
            throw new StarRocksException("User Cancelled");
        }

        StmtExecutor executor = StmtExecutor.newInternalExecutor(ctx, insertStmt);
        ctx.setExecutor(executor);
        if (ctx.getParent() != null && ctx.getParent().getExecutor() != null) {
            StmtExecutor parentStmtExecutor = ctx.getParent().getExecutor();
            parentStmtExecutor.registerSubStmtExecutor(executor);
        }
        ctx.setStmtId(STMT_ID_GENERATOR.incrementAndGet());
        logger.info("[QueryId:{}] start to refresh mv in DML", ctx.getQueryId());
        try {
            executor.handleDMLStmtWithProfile(execPlan, insertStmt);
        } catch (Exception e) {
            logger.warn("[QueryId:{}] refresh mv {} failed in DML", ctx.getQueryId(), e);
            throw e;
        } finally {
            logger.info("[QueryId:{}] finished to refresh mv in DML", ctx.getQueryId());
            auditAfterExec(mvContext, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
        }
    }

    /**
     * Collect all deduplicated databases of the materialized view's base tables.
     *
     * @param mv: the mv to check
     * @return: the deduplicated databases of the materialized view's base tables,
     * throw exception if the database does not exist.
     */
    private LockParams collectDatabases(MaterializedView mv) {
        LockParams lockParams = new LockParams();
        for (BaseTableInfo baseTableInfo : mv.getBaseTableInfos()) {
            Optional<Database> dbOpt =
                    GlobalStateMgr.getCurrentState().getMetadataMgr().getDatabase(new ConnectContext(), baseTableInfo);
            if (dbOpt.isEmpty()) {
                logger.warn("database {} do not exist", baseTableInfo.getDbInfoStr());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }
            Database db = dbOpt.get();
            lockParams.add(db, baseTableInfo.getTableId());
        }
        return lockParams;
    }

    /**
     * Collect all base table snapshot infos for the mv which the snapshot infos are kept and used in the final
     * update meta phase.
     * </p>
     * NOTE:
     * 1. deep copy of the base table's metadata may be time costing, we can optimize it later.
     * 2. no needs to lock the base table's metadata since the metadata is not changed during the refresh process.
     *
     * @param mv the mv to collect
     * @return the base table and its snapshot info map
     */
    @VisibleForTesting
    public Map<Long, TableSnapshotInfo> collectBaseTableSnapshotInfos(MaterializedView mv)
            throws LockTimeoutException {
        Map<Long, TableSnapshotInfo> tables = Maps.newHashMap();
        List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();

        LockParams lockParams = collectDatabases(mv);
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(lockParams, LockType.READ, Config.mv_refresh_try_lock_timeout_ms,
                TimeUnit.MILLISECONDS)) {
            logger.warn("failed to lock database: {} in collectBaseTableSnapshotInfos for mv refresh", lockParams);
            throw new LockTimeoutException("Failed to lock database: " + lockParams + " in collectBaseTableSnapshotInfos");
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            for (BaseTableInfo baseTableInfo : baseTableInfos) {
                Optional<Table> tableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
                if (tableOpt.isEmpty()) {
                    logger.warn("table {} doesn't exist", baseTableInfo.getTableInfoStr());
                    throw new DmlException("Materialized view base table: %s not exist.",
                            baseTableInfo.getTableInfoStr());
                }

                // NOTE: DeepCopy.copyWithGson is very time costing, use `copyOnlyForQuery` to reduce the cost.
                // TODO: Implement a `SnapshotTable` later which can use the copied table or transfer to the real table.
                Table table = tableOpt.get();
                if (table.isNativeTableOrMaterializedView()) {
                    OlapTable copied = null;
                    if (table.isOlapOrCloudNativeTable()) {
                        copied = new OlapTable();
                    } else {
                        copied = new MaterializedView();
                    }
                    OlapTable olapTable = (OlapTable) table;
                    olapTable.copyOnlyForQuery(copied);
                    tables.put(table.getId(), new TableSnapshotInfo(baseTableInfo, copied));
                } else if (table.isView()) {
                    // skip to collect snapshots for views
                } else {
                    // for other table types, use the table directly which needs to lock if visits the table metadata.
                    tables.put(table.getId(), new TableSnapshotInfo(baseTableInfo, table));
                }
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(lockParams, LockType.READ);
        }
        logger.info("collect base table snapshot infos cost: {} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return tables;
    }

    @VisibleForTesting
    public void updateBaseTablePartitionSnapshotInfos(Map<TableSnapshotInfo, Set<String>> refTableAndPartitionNames) {
        // NOTE: For each task run, ref-base table's incremental partition and all non-ref base tables' partitions
        // are refreshed, so we need record it into materialized view.
        // NOTE: We don't use the pruned partition infos from ExecPlan because the optimized partition infos are not
        // exact to describe which partitions are refreshed.
        Map<TableSnapshotInfo, Set<String>> baseTableAndPartitionNames = Maps.newHashMap();
        for (Map.Entry<TableSnapshotInfo, Set<String>> e : refTableAndPartitionNames.entrySet()) {
            TableSnapshotInfo snapshotInfo = e.getKey();
            Table baseTable = snapshotInfo.getBaseTable();
            Set<String> realPartitionNames = e.getValue().stream()
                    .flatMap(name -> mvContext.getExternalTableRealPartitionName(baseTable, name).stream())
                    .collect(Collectors.toSet());
            baseTableAndPartitionNames.put(snapshotInfo, realPartitionNames);
        }

        // Update all base tables' version map to track all tables' changes used in query rewrite.
        Map<TableSnapshotInfo, Set<String>> nonRefTableAndPartitionNames = getNonRefTableRefreshPartitions();
        if (!nonRefTableAndPartitionNames.isEmpty()) {
            baseTableAndPartitionNames.putAll(nonRefTableAndPartitionNames);
        }

        for (Map.Entry<TableSnapshotInfo, Set<String>> e : baseTableAndPartitionNames.entrySet()) {
            TableSnapshotInfo snapshotInfo = e.getKey();
            Set<String> refreshedPartitionNames = e.getValue();
            Map<String, MaterializedView.BasePartitionInfo> refreshedPartitionInfos =
                    getRefreshedPartitionInfos(snapshotInfo, refreshedPartitionNames);
            snapshotInfo.setRefreshedPartitionInfos(refreshedPartitionInfos);
        }
    }

    /**
     * @param mvToRefreshedPartitions :  to-refreshed mv partition names
     * @return : return to-refreshed base table's table name and partition names mapping
     */
    @VisibleForTesting
    public Map<TableSnapshotInfo, Set<String>> getRefTableRefreshPartitions(Set<String> mvToRefreshedPartitions) {
        Map<TableSnapshotInfo, Set<String>> refTableAndPartitionNames = Maps.newHashMap();
        Map<String, Map<Table, Set<String>>> mvToBaseNameRefs = mvContext.getMvRefBaseTableIntersectedPartitions();
        if (mvToBaseNameRefs == null || mvToBaseNameRefs.isEmpty()) {
            return refTableAndPartitionNames;
        }
        for (TableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            Set<String> needRefreshTablePartitionNames = null;
            for (String mvPartitionName : mvToRefreshedPartitions) {
                if (!mvToBaseNameRefs.containsKey(mvPartitionName)) {
                    continue;
                }
                Map<Table, Set<String>> mvToBaseNameRef = mvToBaseNameRefs.get(mvPartitionName);
                if (mvToBaseNameRef.containsKey(snapshotTable)) {
                    if (needRefreshTablePartitionNames == null) {
                        needRefreshTablePartitionNames = Sets.newHashSet();
                    }
                    // The table in this map has related partition with mv
                    // It's ok to add empty set for a table, means no partition corresponding to this mv partition
                    needRefreshTablePartitionNames.addAll(mvToBaseNameRef.get(snapshotTable));
                } else {
                    logger.info("ref-base-table {} is not found in `mvRefBaseTableIntersectedPartitions` " +
                            "because of empty update", snapshotTable.getName());
                }
            }
            if (needRefreshTablePartitionNames != null) {
                refTableAndPartitionNames.put(snapshotInfo, needRefreshTablePartitionNames);
            }
        }
        return refTableAndPartitionNames;
    }

    /**
     * Return all non-ref base table and refreshed partitions.
     */
    private Map<TableSnapshotInfo, Set<String>> getNonRefTableRefreshPartitions() {
        Map<TableSnapshotInfo, Set<String>> tableNamePartitionNames = Maps.newHashMap();
        Map<Table, Map<String, Set<String>>> baseTableToMvNameRefs = mvContext.getRefBaseTableMVIntersectedPartitions();
        for (TableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table table = snapshotInfo.getBaseTable();
            if (baseTableToMvNameRefs != null && baseTableToMvNameRefs.containsKey(table)) {
                // do nothing
            } else {
                if (table.isNativeTableOrMaterializedView()) {
                    tableNamePartitionNames.put(snapshotInfo, ((OlapTable) table).getVisiblePartitionNames());
                } else if (table.isView()) {
                    // do nothing
                } else {
                    tableNamePartitionNames.put(snapshotInfo, Sets.newHashSet(PartitionUtil.getPartitionNames(table)));
                }
            }
        }
        return tableNamePartitionNames;
    }

    /**
     * Collect base table and its refreshed partition infos based on refreshed table infos.
     */
    private Map<String, MaterializedView.BasePartitionInfo> getRefreshedPartitionInfos(
            TableSnapshotInfo snapshotInfo, Set<String> refreshedPartitionNames) {
        Table baseTable = snapshotInfo.getBaseTable();
        BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
        if (baseTable.isNativeTableOrMaterializedView()) {
            Map<String, MaterializedView.BasePartitionInfo> partitionInfos = Maps.newHashMap();
            OlapTable olapTable = (OlapTable) baseTable;
            for (String partitionName : refreshedPartitionNames) {
                Partition partition = olapTable.getPartition(partitionName);
                // it's ok to skip because only existed partitions are updated in the version map.
                if (partition == null) {
                    logger.warn("partition {} not found in base table {}, refreshedPartitionNames:{}",
                            partitionName, baseTable.getName(), refreshedPartitionNames);
                    continue;
                }
                MaterializedView.BasePartitionInfo basePartitionInfo = new MaterializedView.BasePartitionInfo(
                        partition.getId(),
                        partition.getDefaultPhysicalPartition().getVisibleVersion(),
                        partition.getDefaultPhysicalPartition().getVisibleVersionTime());
                partitionInfos.put(partition.getName(), basePartitionInfo);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Collect olap base table {}'s refreshed partition infos: {}", baseTable.getName(), partitionInfos);
            }
            return partitionInfos;
        } else if (MVPCTRefreshPartitioner.isPartitionRefreshSupported(baseTable)) {
            return getSelectedPartitionInfos(baseTable, Lists.newArrayList(refreshedPartitionNames), baseTableInfo);
        } else {
            // FIXME: base table does not support partition-level refresh and does not update the meta
            //  in materialized view.
            logger.warn("refresh mv with non-supported-partition-level refresh base table {}", baseTable.getName());
            return Maps.newHashMap();
        }
    }

    /**
     * @param table                  : input table to collect refresh partition infos
     * @param selectedPartitionNames : input table refreshed partition names
     * @param baseTableInfo          : input table's base table info
     * @return : return the given table's refresh partition infos
     */
    private Map<String, MaterializedView.BasePartitionInfo> getSelectedPartitionInfos(Table table,
                                                                                      List<String> selectedPartitionNames,
                                                                                      BaseTableInfo baseTableInfo) {
        // sort selectedPartitionNames before the for loop, otherwise the order of partition names may be
        // different in selectedPartitionNames and partitions and will lead to infinite partition refresh.
        Collections.sort(selectedPartitionNames);
        Map<String, MaterializedView.BasePartitionInfo> partitionInfos = Maps.newHashMap();
        List<com.starrocks.connector.PartitionInfo> partitions = GlobalStateMgr.
                getCurrentState().getMetadataMgr().getPartitions(baseTableInfo.getCatalogName(), table,
                        selectedPartitionNames);
        for (int index = 0; index < selectedPartitionNames.size(); ++index) {
            long modifiedTime = partitions.get(index).getModifiedTime();
            String partitionName = selectedPartitionNames.get(index);
            MaterializedView.BasePartitionInfo basePartitionInfo =
                    new MaterializedView.BasePartitionInfo(-1, modifiedTime, modifiedTime);
            TableUpdateArbitrator.UpdateContext updateContext = new TableUpdateArbitrator.UpdateContext(
                    table,
                    -1,
                    Lists.newArrayList(partitionName));
            if (table instanceof HiveTable
                    && ((HiveTable) table).getHiveTableType() == HiveTable.HiveTableType.EXTERNAL_TABLE) {
                TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
                if (arbitrator != null) {
                    Map<String, Optional<HivePartitionDataInfo>> partitionDataInfos = arbitrator.getPartitionDataInfos();
                    Preconditions.checkState(partitionDataInfos.size() == 1);
                    if (partitionDataInfos.get(partitionName).isPresent()) {
                        HivePartitionDataInfo hivePartitionDataInfo = partitionDataInfos.get(partitionName).get();
                        basePartitionInfo.setExtLastFileModifiedTime(hivePartitionDataInfo.getLastFileModifiedTime());
                        basePartitionInfo.setFileNumber(hivePartitionDataInfo.getFileNumber());
                    }
                }
            }
            partitionInfos.put(partitionName, basePartitionInfo);
        }
        return partitionInfos;
    }

    @VisibleForTesting
    public Map<Long, TableSnapshotInfo> getSnapshotBaseTables() {
        return snapshotBaseTables;
    }

    private String getPostRun(ConnectContext ctx, MaterializedView mv) {
        // check whether it's enabled to analyze MV task after task run for each task run,
        // so the analyze_for_mv can be set in session variable dynamically
        if (mv == null) {
            return "";
        }
        return TaskBuilder.getAnalyzeMVStmt(ctx, mv.getName());
    }

    @Override
    public void postTaskRun(TaskRunContext context) throws Exception {
        if (!isNeedToTriggerPostProcess) {
            return;
        }
        final ConnectContext ctx = context.getCtx();
        try (var ignored = ctx.bindScope()) {
            // trigger mv into plan cache into plan cache
            CachingMvPlanContextBuilder.getInstance().loadPlanContextAsync(mv);

            // recreate post run context for each task run
            final String postRun = getPostRun(ctx, mv);
            // visible for tests
            if (mvContext != null) {
                mvContext.setPostRun(postRun);
            }
            context.setPostRun(postRun);
            if (StringUtils.isNotEmpty(postRun)) {
                ctx.executeSql(postRun);
            }
        }
    }
}