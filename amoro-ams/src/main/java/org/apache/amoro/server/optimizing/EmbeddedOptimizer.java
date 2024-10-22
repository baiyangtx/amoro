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

import org.apache.amoro.OptimizerProperties;
import org.apache.amoro.api.OptimizingService;
import org.apache.amoro.optimizer.common.OptimizerConfig;
import org.apache.amoro.optimizer.common.OptimizerExecutor;
import org.apache.amoro.optimizer.common.OptimizerToucher;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.utils.CompatiblePropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class EmbeddedOptimizer {
  public static final String PROPERTY_IS_EMBEDDED = "is-embedded";
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedOptimizer.class);
  private final List<OptimizerExecutor> executors;
  private final EmbeddedOptimizerToucher toucher;

  private String groupName;
  public EmbeddedOptimizer(ResourceGroup group, OptimizingService.Iface service) {
    this.groupName = group.getName();
    int parallel = CompatiblePropertyUtil.propertyAsInt(
        group.getProperties(),
        OptimizerProperties.EMBEDDED_OPTIMIZER_PARALLEL,
        OptimizerProperties.EMBEDDED_OPTIMIZER_PARALLEL_DEFAULT);
    OptimizerConfig config = new OptimizerConfig();
    config.setAmsUrl("");
    config.setExecutionParallel(parallel);
    config.setMemorySize(0);
    config.setGroupName(groupName);

    executors = IntStream.range(0, parallel).boxed()
        .map(i -> new EmbeddedOptimizerExecutor(service, config, i))
        .collect(Collectors.toList());
    toucher = new EmbeddedOptimizerToucher(service, config);
    toucher.withTokenChangeListener(t -> executors.forEach(e -> e.setToken(t)));
    toucher.withRegisterProperty(PROPERTY_IS_EMBEDDED, "true");
  }

  public void start() {
    if (executors.isEmpty()) {
      LOG.info("Executors is empty, skip start embedded optimizer, group:{}", groupName);
      return;
    }
    executors.forEach(e -> {
      String name = "Embedded-optimizer-" + groupName + "-executor-" + e.getThreadId();
      Thread thread = new Thread(e::start, name);
      thread.start();
      LOG.info("Start thread: {}", name);
    });
    Thread toucherThread = new Thread(toucher::start,
        "Embedded-optimizer-" + groupName + "-toucher");
    toucherThread.start();
    LOG.info("Start thread:{}", toucherThread.getName());
  }

  public void dispose() {
    if (executors.isEmpty()) {
      LOG.info("Executors is empty, skip dispose embedded optimizer, group:{}", groupName);
      return;
    }
    executors.forEach(e -> e.stop());
  }


  private static final class EmbeddedOptimizerExecutor extends OptimizerExecutor {
    private final OptimizingService.Iface service;
    public EmbeddedOptimizerExecutor(OptimizingService.Iface service, OptimizerConfig config, int threadId) {
      super(config, threadId);
      this.service = service;
    }

    @Override
    protected OptimizingService.Iface getClient() {
      return service;
    }
  }

  private static final class EmbeddedOptimizerToucher extends OptimizerToucher {
    private final OptimizingService.Iface service;
    public EmbeddedOptimizerToucher(OptimizingService.Iface service, OptimizerConfig config) {
      super(config);
      this.service = service;
    }

    @Override
    protected OptimizingService.Iface getClient() {
      return service;
    }
  }
}
