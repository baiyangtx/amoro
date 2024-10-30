/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.server.optimizing;

import org.apache.amoro.api.OptimizingTaskId;
import org.apache.amoro.exception.OptimizingClosedException;
import org.apache.amoro.optimizing.RewriteFilesInput;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.process.SimpleFuture;
import org.apache.amoro.server.AmoroServiceConstants;
import org.apache.amoro.server.optimizing.plan.OptimizingPlanner;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TaskFilesPersistence;
import org.apache.amoro.server.persistence.mapper.OptimizingMapper;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.server.table.TableRuntime;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.utils.ExceptionUtil;
import org.apache.amoro.utils.MixedDataFiles;
import org.apache.amoro.utils.TablePropertyUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class TableOptimizingProcess extends PersistentBase
    implements OptimizingProcess, TaskRuntime.TaskOwner {
  private static final Logger LOG = LoggerFactory.getLogger(TableOptimizingProcess.class);
  private final long processId;
  private final OptimizingType optimizingType;
  private final TableRuntime tableRuntime;
  private final long planTime;
  private final long targetSnapshotId;
  private final long targetChangeSnapshotId;
  private final Map<OptimizingTaskId, TaskRuntime<RewriteStageTask>> taskMap = Maps.newHashMap();
  private final Queue<TaskRuntime<RewriteStageTask>> taskQueue = new LinkedList<>();
  private final Queue<TaskRuntime> retryTaskQueue = new LinkedList<>();
  private final Lock lock = new ReentrantLock();
  private volatile ProcessStatus status = ProcessStatus.RUNNING;
  private volatile String failedReason;
  private long endTime = AmoroServiceConstants.INVALID_TIME;
  private Map<String, Long> fromSequence = Maps.newHashMap();
  private Map<String, Long> toSequence = Maps.newHashMap();
  private boolean hasCommitted = false;
  private final TableManager tableManager;
  private final SimpleFuture completeFuture = new SimpleFuture();

  public TaskRuntime poll(TaskRuntime.Classifier classifier) {
    lock.lock();
    try {
      switch (classifier) {
        case SCHEDULED_REWRITE:
          return taskQueue.poll();
        case RETRY_REWRITE:
          return retryTaskQueue.poll();
        default:
          return null;
      }
    } finally {
      lock.unlock();
    }
  }

  public TableOptimizingProcess(
      OptimizingPlanner planner, TableRuntime tableRuntime, TableManager tableManager) {
    this.tableManager = tableManager;
    this.tableRuntime = tableRuntime;
    processId = planner.getProcessId();
    optimizingType = planner.getOptimizingType();
    planTime = planner.getPlanTime();
    targetSnapshotId = planner.getTargetSnapshotId();
    targetChangeSnapshotId = planner.getTargetChangeSnapshotId();
    loadTaskRuntimes(planner.planTasks());
    fromSequence = planner.getFromSequence();
    toSequence = planner.getToSequence();
    beginAndPersistProcess();
  }

  public TableOptimizingProcess(TableRuntime tableRuntime, TableManager tableManager) {
    this.tableManager = tableManager;
    processId = tableRuntime.getProcessId();
    this.tableRuntime = tableRuntime;
    optimizingType = tableRuntime.getOptimizingType();
    targetSnapshotId = tableRuntime.getTargetSnapshotId();
    targetChangeSnapshotId = tableRuntime.getTargetChangeSnapshotId();
    planTime = tableRuntime.getLastPlanTime();
    if (tableRuntime.getFromSequence() != null) {
      fromSequence = tableRuntime.getFromSequence();
    }
    if (tableRuntime.getToSequence() != null) {
      toSequence = tableRuntime.getToSequence();
    }
    if (this.status != ProcessStatus.CLOSED) {
      tableRuntime.recover(this);
    }
    loadTaskRuntimes(this);
  }

  @Override
  public long getProcessId() {
    return processId;
  }

  @Override
  public OptimizingType getOptimizingType() {
    return optimizingType;
  }

  @Override
  public ProcessStatus getStatus() {
    return status;
  }

  @Override
  public void close() {
    lock.lock();
    try {
      if (this.status != ProcessStatus.RUNNING) {
        return;
      }
      this.status = ProcessStatus.CLOSED;
      this.endTime = System.currentTimeMillis();
      persistProcessCompleted(false);
      completeFuture.complete();
    } finally {
      lock.unlock();
    }
    releaseResourcesIfNecessary();
  }

  @Override
  public void acceptResult(TaskRuntime taskRuntime) {
    lock.lock();
    try {
      try {
        tableRuntime.addTaskQuota(taskRuntime.getCurrentQuota());
      } catch (Throwable throwable) {
        LOG.warn(
            "{} failed to add task quota {}, ignore it",
            tableRuntime.getTableIdentifier(),
            taskRuntime.getTaskId(),
            throwable);
      }
      if (isClosed()) {
        throw new OptimizingClosedException(processId);
      }
      if (taskRuntime.getStatus() == TaskRuntime.Status.SUCCESS) {
        // the lock of TableOptimizingProcess makes it thread-safe
        if (allTasksPrepared()
            && tableRuntime.getOptimizingStatus().isProcessing()
            && tableRuntime.getOptimizingStatus() != OptimizingStatus.COMMITTING) {
          tableRuntime.beginCommitting();
          completeFuture.complete();
        }
      } else if (taskRuntime.getStatus() == TaskRuntime.Status.FAILED) {
        if (taskRuntime.getRetry() < tableRuntime.getMaxExecuteRetryCount()) {
          LOG.info(
              "Put task {} to retry queue, because {}",
              taskRuntime.getTaskId(),
              taskRuntime.getFailReason());
          retryTaskQueue.add(taskRuntime);
        } else {
          completeFuture.complete();
          this.failedReason = taskRuntime.getFailReason();
          this.status = ProcessStatus.FAILED;
          this.endTime = taskRuntime.getEndTime();
          persistProcessCompleted(false);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  // the cleanup of task should be done after unlock to avoid deadlock
  @Override
  public void releaseResourcesIfNecessary() {
    if (this.status == ProcessStatus.FAILED || this.status == ProcessStatus.CLOSED) {
      cancelTasks();
    }
  }

  @Override
  public boolean isClosed() {
    return status == ProcessStatus.CLOSED;
  }

  @Override
  public long getPlanTime() {
    return planTime;
  }

  @Override
  public long getDuration() {
    long dur =
        endTime == AmoroServiceConstants.INVALID_TIME
            ? System.currentTimeMillis() - planTime
            : endTime - planTime;
    return Math.max(0, dur);
  }

  @Override
  public long getTargetSnapshotId() {
    return targetSnapshotId;
  }

  @Override
  public long getTargetChangeSnapshotId() {
    return targetChangeSnapshotId;
  }

  @Override
  public SimpleFuture getCompleteFuture() {
    return completeFuture;
  }

  public void retryTask(TaskRuntime task) {
    retryTaskQueue.add(task);
  }

  public String getFailedReason() {
    return failedReason;
  }

  public Map<OptimizingTaskId, TaskRuntime<RewriteStageTask>> getTaskMap() {
    return taskMap;
  }

  /**
   * if all tasks are Prepared
   *
   * @return true if tasks is not empty and all Prepared
   */
  private boolean allTasksPrepared() {
    if (!taskMap.isEmpty()) {
      return taskMap.values().stream().allMatch(t -> t.getStatus() == TaskRuntime.Status.SUCCESS);
    }
    return false;
  }

  /**
   * Get optimizeRuntime.
   *
   * @return -
   */
  @Override
  public long getRunningQuotaTime(long calculatingStartTime, long calculatingEndTime) {
    return taskMap.values().stream()
        .filter(t -> !t.finished())
        .mapToLong(task -> task.getQuotaTime(calculatingStartTime, calculatingEndTime))
        .sum();
  }

  @Override
  public void commit() {
    LOG.debug(
        "{} get {} tasks of {} partitions to commit",
        tableRuntime.getTableIdentifier(),
        taskMap.size(),
        taskMap.values());

    lock.lock();
    try {
      if (hasCommitted) {
        LOG.warn("{} has already committed, give up", tableRuntime.getTableIdentifier());
        throw new IllegalStateException("repeat commit, and last error " + failedReason);
      }
      try {
        hasCommitted = true;
        buildCommit().commit();
        status = ProcessStatus.SUCCESS;
        endTime = System.currentTimeMillis();
        persistProcessCompleted(true);
      } catch (Exception e) {
        LOG.error("{} Commit optimizing failed ", tableRuntime.getTableIdentifier(), e);
        status = ProcessStatus.FAILED;
        failedReason = ExceptionUtil.getErrorMessage(e, 4000);
        endTime = System.currentTimeMillis();
        persistProcessCompleted(false);
      } finally {
        completeFuture.complete();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public MetricsSummary getSummary() {
    return new MetricsSummary(taskMap.values());
  }

  private UnKeyedTableCommit buildCommit() {
    MixedTable table =
        (MixedTable) tableManager.loadTable(tableRuntime.getTableIdentifier()).originalTable();
    if (table.isUnkeyedTable()) {
      return new UnKeyedTableCommit(targetSnapshotId, table, taskMap.values());
    } else {
      return new KeyedTableCommit(
          table,
          taskMap.values(),
          targetSnapshotId,
          convertPartitionSequence(table, fromSequence),
          convertPartitionSequence(table, toSequence));
    }
  }

  private StructLikeMap<Long> convertPartitionSequence(
      MixedTable table, Map<String, Long> partitionSequence) {
    PartitionSpec spec = table.spec();
    StructLikeMap<Long> results = StructLikeMap.create(spec.partitionType());
    partitionSequence.forEach(
        (partition, sequence) -> {
          if (spec.isUnpartitioned()) {
            results.put(TablePropertyUtil.EMPTY_STRUCT, sequence);
          } else {
            StructLike partitionData = MixedDataFiles.data(spec, partition);
            results.put(partitionData, sequence);
          }
        });
    return results;
  }

  private void beginAndPersistProcess() {
    doAsTransaction(
        () ->
            doAs(
                OptimizingMapper.class,
                mapper ->
                    mapper.insertOptimizingProcess(
                        tableRuntime.getTableIdentifier(),
                        processId,
                        targetSnapshotId,
                        targetChangeSnapshotId,
                        status,
                        optimizingType,
                        planTime,
                        getSummary(),
                        fromSequence,
                        toSequence)),
        () ->
            doAs(
                OptimizingMapper.class,
                mapper -> mapper.insertTaskRuntimes(Lists.newArrayList(taskMap.values()))),
        () -> TaskFilesPersistence.persistTaskInputs(processId, taskMap.values()),
        () -> tableRuntime.beginProcess(this));
  }

  private void persistProcessCompleted(boolean success) {
    doAsTransaction(
        () ->
            doAs(
                OptimizingMapper.class,
                mapper ->
                    mapper.updateOptimizingProcess(
                        tableRuntime.getTableIdentifier().getId(),
                        processId,
                        status,
                        endTime,
                        getSummary(),
                        getFailedReason())),
        () -> tableRuntime.completeProcess(success));
  }

  /** The cancellation should be invoked outside the process lock to avoid deadlock. */
  private void cancelTasks() {
    taskMap.values().forEach(TaskRuntime::tryCanceling);
  }

  private void loadTaskRuntimes(OptimizingProcess optimizingProcess) {
    try {
      List<TaskRuntime<RewriteStageTask>> taskRuntimes =
          getAs(
              OptimizingMapper.class,
              mapper ->
                  mapper.selectTaskRuntimes(tableRuntime.getTableIdentifier().getId(), processId));
      Map<Integer, RewriteFilesInput> inputs = TaskFilesPersistence.loadTaskInputs(processId);
      taskRuntimes.forEach(
          taskRuntime -> {
            taskRuntime.claimOwnership(this);
            taskRuntime
                .getTaskDescriptor()
                .setInput(inputs.get(taskRuntime.getTaskId().getTaskId()));
            taskMap.put(taskRuntime.getTaskId(), taskRuntime);
            if (taskRuntime.getStatus() == TaskRuntime.Status.PLANNED) {
              taskQueue.offer(taskRuntime);
            } else if (taskRuntime.getStatus() == TaskRuntime.Status.FAILED) {
              retryTaskQueue.add(taskRuntime);
            }
          });
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "Load task inputs failed, close the optimizing process : {}",
          optimizingProcess.getProcessId(),
          e);
      optimizingProcess.close();
    }
  }

  private void loadTaskRuntimes(List<RewriteStageTask> taskDescriptors) {
    int taskId = 1;
    for (RewriteStageTask taskDescriptor : taskDescriptors) {
      TaskRuntime<RewriteStageTask> taskRuntime =
          new TaskRuntime<>(new OptimizingTaskId(processId, taskId++), taskDescriptor);
      LOG.info(
          "{} plan new task {}, summary {}",
          tableRuntime.getTableIdentifier(),
          taskRuntime.getTaskId(),
          taskRuntime.getSummary());
      taskMap.put(taskRuntime.getTaskId(), taskRuntime.claimOwnership(this));
      taskQueue.offer(taskRuntime);
    }
  }
}
