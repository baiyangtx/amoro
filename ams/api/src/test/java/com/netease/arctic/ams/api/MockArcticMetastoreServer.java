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

package com.netease.arctic.ams.api;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.netease.arctic.ams.api.properties.CatalogMetaProperties;
import io.javalin.Javalin;
import io.javalin.apibuilder.EndpointGroup;
import io.javalin.http.ConflictResponse;
import io.javalin.http.ContentType;
import io.javalin.http.Context;
import io.javalin.http.NotFoundResponse;
import io.javalin.http.ServiceUnavailableResponse;
import io.javalin.plugin.json.JavalinJackson;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.eclipse.jetty.server.session.SessionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.javalin.apibuilder.ApiBuilder.delete;
import static io.javalin.apibuilder.ApiBuilder.get;
import static io.javalin.apibuilder.ApiBuilder.head;
import static io.javalin.apibuilder.ApiBuilder.path;
import static io.javalin.apibuilder.ApiBuilder.post;

public class MockArcticMetastoreServer {
  private static final Logger LOG = LoggerFactory.getLogger(MockArcticMetastoreServer.class);
  public static final String TEST_CATALOG_NAME = "test_catalog";
  public static final String TEST_DB_NAME = "test_db";

  private int thriftPort;
  private int restPort;

  private int retry = 10;
  private boolean started = false;
  private final AmsHandler amsHandler = new AmsHandler();

  private final OptimizerManagerHandler optimizerManagerHandler = new OptimizerManagerHandler();

  private final MockStore mockStore = new MockStore();
  private TServer thriftServer;
  private Javalin restServer;

  private static final MockArcticMetastoreServer INSTANCE = new MockArcticMetastoreServer();

  public static MockArcticMetastoreServer getInstance() {
    if (!INSTANCE.isStarted()) {
      INSTANCE.start();
    }
    return INSTANCE;
  }


  public static String getHadoopSite() {
    return Base64.getEncoder().encodeToString("<configuration></configuration>".getBytes(StandardCharsets.UTF_8));
  }

  public String getThriftUrl() {
    return "thrift://127.0.0.1:" + thriftPort;
  }

  public String getThriftUrl(String catalogName) {
    return "thrift://127.0.0.1:" + thriftPort + "/" + catalogName;
  }

  public MockArcticMetastoreServer() {
    this.thriftPort = randomPort();
    this.restPort = randomPort();
  }

  int randomPort() {
    // create a random port between 14000 - 18000
    int port = new Random().nextInt(4000);
    return port + 14000;
  }

  public void start() {
    Thread thriftServer = new Thread(new ThriftServer());
    Thread restServer = new Thread(new RestServer());
    thriftServer.start();
    restServer.start();
    started = true;
  }

  public void stopAndCleanUp() {
    if (thriftServer != null) {
      thriftServer.stop();
    }
    if (restServer != null) {
      restServer.stop();
    }
    mockStore.reset();
    started = false;
  }

  public void reset() {
    mockStore.reset();
  }

  public boolean isStarted() {
    return started;
  }

  public AmsHandler handler() {
    return amsHandler;
  }

  public OptimizerManagerHandler optimizerHandler() {
    return optimizerManagerHandler;
  }

  public int port() {
    return thriftPort;
  }


  private class RestServer implements Runnable {

    @Override
    public void run() {
      restServer = Javalin.create(config -> {
        config.sessionHandler(SessionHandler::new);
        config.enableCorsForAllOrigins();
        config.showJavalinBanner = false;
      });
      IcebergRestCatalogService restCatalogService = new IcebergRestCatalogService();
      restServer.routes(() -> {
        restCatalogService.endpoints().addEndpoints();
      });

      restServer.start(restPort);
    }
  }

  private static class MockStore {
    public final ConcurrentLinkedQueue<CatalogMeta> catalogs = new ConcurrentLinkedQueue<>();
    public final ConcurrentLinkedQueue<TableMeta> tables = new ConcurrentLinkedQueue<>();
    public final ConcurrentHashMap<String, List<String>> databases = new ConcurrentHashMap<>();

    public final Map<TableIdentifier, List<TableCommitMeta>> tableCommitMetas = new HashMap<>();

    public final Map<TableIdentifier, Map<String, Blocker>> tableBlockers = new HashMap<>();
    public final AtomicLong blockerId = new AtomicLong(1L);

    public void reset() {
      catalogs.clear();
      tables.clear();
      databases.clear();
      tableCommitMetas.clear();
    }

    public CatalogMeta getCatalog(String name) throws NoSuchObjectException {
      return catalogs.stream().filter(c -> name.equals(c.getCatalogName()))
          .findFirst().orElseThrow(() -> new NoSuchObjectException("catalog with name: " + name + " non-exists."));
    }

    public List<String> getDatabases(String catalogName) {
      return databases.get(catalogName) == null ? new ArrayList<>() : databases.get(catalogName);
    }

    public void createDatabase(String catalogName, String database) throws AlreadyExistsException {
      databases.computeIfAbsent(catalogName, c -> new ArrayList<>());
      if (databases.get(catalogName).contains(database)) {
        throw new AlreadyExistsException("database exist");
      }
      databases.computeIfPresent(catalogName, (c, dbList) -> {
        List<String> newList = new ArrayList<>(dbList);
        newList.add(database);
        return newList;
      });
    }

    public void dropDatabase(String catalogName, String database) throws NoSuchObjectException {
      List<String> dbList = databases.get(catalogName);
      if (dbList == null || !dbList.contains(database)) {
        throw new NoSuchObjectException();
      }
      databases.computeIfPresent(catalogName, (c, dbs) -> {
        List<String> databaseList = new ArrayList<>(dbs);
        databaseList.remove(database);
        return databaseList;
      });
    }
  }

  private class ThriftServer implements Runnable {

    @Override
    public void run() {
      try {
        TServerSocket socket = new TServerSocket(thriftPort);
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        ArcticTableMetastore.Processor<AmsHandler> amsProcessor =
            new ArcticTableMetastore.Processor<>(amsHandler);
        processor.registerProcessor("TableMetastore", amsProcessor);

        OptimizingService.Processor<OptimizerManagerHandler> optimizerManProcessor =
            new OptimizingService.Processor<>(optimizerManagerHandler);
        processor.registerProcessor("OptimizeManager", optimizerManProcessor);

        final long maxMessageSize = 100 * 1024 * 1024L;
        final TProtocolFactory protocolFactory;
        final TProtocolFactory inputProtoFactory;
        protocolFactory = new TBinaryProtocol.Factory();
        inputProtoFactory = new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize);

        SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<>();
        AtomicInteger threadCount = new AtomicInteger(0);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
            1,
            10,
            60,
            TimeUnit.SECONDS,
            executorQueue,
            r -> {
              Thread thread = new Thread(r);
              String threadName = "AMS-pool-" + threadCount.incrementAndGet();
              thread.setName(threadName);
              LOG.info("Mock AMS create thread: " + threadName);
              return thread;
            }, new ThreadPoolExecutor.AbortPolicy());

        TThreadPoolServer.Args args = new TThreadPoolServer.Args(socket)
            .processor(processor)
            .transportFactory(new TFramedTransport.Factory())
            .protocolFactory(protocolFactory)
            .inputProtocolFactory(inputProtoFactory)
            .executorService(threadPoolExecutor);
        thriftServer = new TThreadPoolServer(args);
        thriftServer.serve();

        LOG.info("arctic in-memory metastore start");
      } catch (TTransportException e) {
        if (e.getCause() instanceof BindException) {
          if (--retry < 0) {
            throw new IllegalStateException(e);
          } else {
            thriftPort = randomPort();
            LOG.info("Address already in use, port {}, and retry a new port.", thriftPort);
            run();
          }
        } else {
          throw new IllegalStateException(e);
        }
      }
    }
  }

  public class AmsHandler implements ArcticTableMetastore.Iface {
    private static final long DEFAULT_BLOCKER_TIMEOUT = 60_000;


    public void createCatalog(CatalogMeta catalogMeta) {
      dropCatalog(catalogMeta.getCatalogName());
      mockStore.catalogs.add(catalogMeta);
    }

    public void dropCatalog(String catalogName) {
      mockStore.tables.removeIf(tableMeta -> tableMeta.getTableIdentifier().getCatalog().equals(catalogName));
      mockStore.databases.remove(catalogName);
      mockStore.catalogs.removeIf(catalogMeta -> catalogMeta.getCatalogName().equals(catalogName));
    }

    public Map<TableIdentifier, List<TableCommitMeta>> getTableCommitMetas() {
      return mockStore.tableCommitMetas;
    }

    @Override
    public void ping() {

    }

    @Override
    public List<CatalogMeta> getCatalogs() {
      return new ArrayList<>(mockStore.catalogs);
    }

    @Override
    public CatalogMeta getCatalog(String name) throws TException {
      return mockStore.getCatalog(name);
    }

    @Override
    public List<String> getDatabases(String catalogName) throws TException {
      return mockStore.getDatabases(catalogName);
    }

    @Override
    public void createDatabase(String catalogName, String database) throws TException {
      mockStore.createDatabase(catalogName, database);
    }

    @Override
    public void dropDatabase(String catalogName, String database) throws TException {
      mockStore.dropDatabase(catalogName, database);
    }

    @Override
    public void createTableMeta(TableMeta tableMeta)
        throws TException {
      TableIdentifier identifier = tableMeta.getTableIdentifier();
      String catalog = identifier.getCatalog();
      String database = identifier.getDatabase();
      CatalogMeta catalogMeta = getCatalog(catalog);
      if (
          !"hive".equalsIgnoreCase(catalogMeta.getCatalogType()) &&
              (mockStore.databases.get(catalog) == null || !mockStore.databases.get(catalog).contains(database))) {
        throw new NoSuchObjectException("database non-exists");
      }
      mockStore.tables.add(tableMeta);
    }

    @Override
    public List<TableMeta> listTables(String catalogName, String database) throws TException {
      return mockStore.tables.stream()
          .filter(t -> catalogName.equals(t.getTableIdentifier().getCatalog()))
          .filter(t -> database.equals(t.getTableIdentifier().getDatabase()))
          .collect(Collectors.toList());
    }

    @Override
    public TableMeta getTable(TableIdentifier tableIdentifier) throws TException {
      return mockStore.tables.stream()
          .filter(t -> tableIdentifier.equals(t.getTableIdentifier()))
          .findFirst()
          .orElseThrow(NoSuchObjectException::new);
    }

    @Override
    public void removeTable(TableIdentifier tableIdentifier, boolean deleteData) {
      mockStore.tables.removeIf(t -> t.getTableIdentifier().equals(tableIdentifier));
    }

    @Override
    public void tableCommit(TableCommitMeta commit) throws TException {
      mockStore.tableCommitMetas.putIfAbsent(commit.getTableIdentifier(), new ArrayList<>());
      mockStore.tableCommitMetas.get(commit.getTableIdentifier()).add(commit);
      if (commit.getProperties() != null) {
        TableMeta meta = getTable(commit.getTableIdentifier());
        meta.setProperties(commit.getProperties());
      }
    }

    @Override
    public long allocateTransactionId(TableIdentifier tableIdentifier, String transactionSignature) {
      throw new UnsupportedOperationException("allocate TransactionId from AMS is not supported now");
    }

    @Override
    public Blocker block(
        TableIdentifier tableIdentifier, List<BlockableOperation> operations,
        Map<String, String> properties) throws TException {
      Map<String, Blocker> blockers = mockStore.tableBlockers.computeIfAbsent(tableIdentifier, t -> new HashMap<>());
      long now = System.currentTimeMillis();
      properties.put("create.time", now + "");
      properties.put("expiration.time", (now + DEFAULT_BLOCKER_TIMEOUT) + "");
      properties.put("blocker.timeout", DEFAULT_BLOCKER_TIMEOUT + "");
      Blocker blocker = new Blocker(mockStore.blockerId.getAndIncrement() + "", operations, properties);
      blockers.put(blocker.getBlockerId(), blocker);
      return blocker;
    }

    @Override
    public void releaseBlocker(TableIdentifier tableIdentifier, String blockerId) {
      Map<String, Blocker> blockers = mockStore.tableBlockers.get(tableIdentifier);
      if (blockers != null) {
        blockers.remove(blockerId);
      }
    }

    @Override
    public long renewBlocker(TableIdentifier tableIdentifier, String blockerId) throws TException {
      Map<String, Blocker> blockers = mockStore.tableBlockers.get(tableIdentifier);
      if (blockers == null) {
        throw new NoSuchObjectException("illegal blockerId " + blockerId + ", it may be released or expired");
      }
      Blocker blocker = blockers.get(blockerId);
      if (blocker == null) {
        throw new NoSuchObjectException("illegal blockerId " + blockerId + ", it may be released or expired");
      }
      long expirationTime = System.currentTimeMillis() + DEFAULT_BLOCKER_TIMEOUT;
      blocker.getProperties().put("expiration.time", expirationTime + "");
      return expirationTime;
    }

    @Override
    public List<Blocker> getBlockers(TableIdentifier tableIdentifier) throws TException {
      Map<String, Blocker> blockers = mockStore.tableBlockers.get(tableIdentifier);
      if (blockers == null) {
        return Collections.emptyList();
      } else {
        return new ArrayList<>(blockers.values());
      }
    }

    public void updateMeta(CatalogMeta meta, String key, String value) {
      meta.getCatalogProperties().put(key, value);
    }
  }

  public static class OptimizerManagerHandler implements OptimizingService.Iface {

    private final Map<String, OptimizerRegisterInfo> registeredOptimizers = new ConcurrentHashMap<>();
    private final Queue<OptimizingTask> pendingTasks = new ArrayBlockingQueue<>(100);
    private final Map<String, Map<Integer, OptimizingTaskId>> executingTasks = new ConcurrentHashMap<>();
    private final Map<String, List<OptimizingTaskResult>> completedTasks = new ConcurrentHashMap<>();

    public void cleanUp() {
    }

    @Override
    public void ping() {

    }

    @Override
    public void touch(String authToken) throws TException {
      checkToken(authToken);
    }

    @Override
    public OptimizingTask pollTask(String authToken, int threadId) throws TException {
      checkToken(authToken);
      return pendingTasks.poll();
    }

    @Override
    public void ackTask(String authToken, int threadId, OptimizingTaskId taskId) throws TException {
      checkToken(authToken);
      if (!executingTasks.containsKey(authToken)) {
        executingTasks.putIfAbsent(authToken, new ConcurrentHashMap<>());
      }
      Map<Integer, OptimizingTaskId> executingTasksMap = executingTasks.get(authToken);
      if (executingTasksMap.containsKey(threadId)) {
        throw new ArcticException(ErrorCodes.DUPLICATED_TASK_ERROR_CODE, "DuplicateTask", String.format("Optimizer:%s" +
            " thread:%d is executing another task", authToken, threadId));
      }
      executingTasksMap.put(threadId, taskId);
    }

    @Override
    public void completeTask(String authToken, OptimizingTaskResult taskResult) throws TException {
      checkToken(authToken);
      executingTasks.get(authToken).remove(taskResult.getThreadId());
      if (!completedTasks.containsKey(authToken)) {
        completedTasks.putIfAbsent(authToken, new CopyOnWriteArrayList<>());
      }
      List<OptimizingTaskResult> completeTaskList = completedTasks.get(authToken);
      completeTaskList.add(taskResult);
    }

    @Override
    public String authenticate(OptimizerRegisterInfo registerInfo) throws TException {
      String token = UUID.randomUUID().toString();
      registeredOptimizers.put(token, registerInfo);
      return token;
    }

    public Map<String, OptimizerRegisterInfo> getRegisteredOptimizers() {
      return registeredOptimizers;
    }

    public boolean offerTask(OptimizingTask task) {
      return pendingTasks.offer(task);
    }

    public Queue<OptimizingTask> getPendingTasks() {
      return pendingTasks;
    }

    public Map<String, Map<Integer, OptimizingTaskId>> getExecutingTasks() {
      return executingTasks;
    }

    public Map<String, List<OptimizingTaskResult>> getCompletedTasks() {
      return completedTasks;
    }

    private void checkToken(String token) throws ArcticException {
      if (!registeredOptimizers.containsKey(token)) {
        throw new ArcticException(ErrorCodes.PLUGIN_RETRY_AUTH_ERROR_CODE, "unknown token", "unknown token");
      }
    }
  }

  private class IcebergRestCatalogService {
    private final JavalinJackson jsonMapper;
    public static final String ICEBERG_REST_API_PREFIX = "/api/iceberg/rest";

    public IcebergRestCatalogService() {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
      objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      objectMapper.setPropertyNamingStrategy(new PropertyNamingStrategy.KebabCaseStrategy());
      RESTSerializers.registerAll(objectMapper);
      this.jsonMapper = new JavalinJackson(objectMapper);
    }

    public EndpointGroup endpoints() {
      return () -> {
        // for iceberg rest catalog api
        path(ICEBERG_REST_API_PREFIX, () -> {
          get("/v1/config", this::getCatalogConfig);
          get("/v1/catalogs/{catalog}/namespaces", this::listNamespaces);
          post("/v1/catalogs/{catalog}/namespaces", this::createNamespace);
          get("/v1/catalogs/{catalog}/namespaces/{namespace}", this::getNamespace);
          delete("/v1/catalogs/{catalog}/namespaces/{namespace}", this::dropNamespace);
          post("/v1/catalogs/{catalog}/namespaces/{namespace}", this::setNamespaceProperties);
          get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables", this::listTablesInNamespace);
          post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables", this::createTable);
          get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}", this::loadTable);
          post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}", this::commitTable);
          delete("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}", this::deleteTable);
          head("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}", this::tableExists);
          post("/v1/catalogs/{catalog}/tables/rename", this::renameTable);
          post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/metrics", this::metricReport);
        });
      };
    }

    /**
     * GET PREFIX/v1/config?warehouse={warehouse}
     */
    public void getCatalogConfig(Context ctx) {
      String name = ctx.req.getParameter("warehouse");
      RESTResponse restResponse = mockStore.catalogs.stream().filter(c -> name.equals(c.getCatalogName()))
          .filter(c -> CatalogMetaProperties.CATALOG_TYPE_AMS.equalsIgnoreCase(c.getCatalogType()))
          .findFirst()
          .map(c -> ConfigResponse.builder().
              withOverrides(ImmutableMap.of("prefix", "catalogs/" + name))
                .build())
          .orElseThrow(() -> new NotFoundResponse("Internal Catalog: " + name + " not found"));
      jsonResponse(ctx, restResponse);
    }

    /**
     * GET PREFIX/{catalog}/v1/namespaces
     */
    public void listNamespaces(Context ctx) {
      String catalog = ctx.pathParam("catalog");
      String ns = ctx.req.getParameter("parent");
      RESTResponse response;
      List<Namespace> nsLists = Lists.newArrayList();
      if (ns == null) {
        nsLists = mockStore.getDatabases(catalog)
            .stream().map(Namespace::of)
            .collect(Collectors.toList());
      }
      response = ListNamespacesResponse.builder()
          .addAll(nsLists)
          .build();
      jsonResponse(ctx, response);
    }

    /**
     * POST PREFIX/{catalog}/v1/namespaces
     */
    public void createNamespace(Context ctx) {
      CreateNamespaceRequest request = bodyAsClass(ctx, CreateNamespaceRequest.class);
      Namespace ns = request.namespace();
      String catalog = ctx.pathParam("catalog");
      try {
        amsHandler.createDatabase(catalog, ns.level(0));
      } catch (AlreadyExistsException e) {
        throw new ConflictResponse(e.getMessage());
      } catch (TException e) {
        throw new ServiceUnavailableResponse(e.getMessage());
      }
      jsonResponse(ctx, CreateNamespaceResponse.builder().withNamespace(ns).build());
    }

    /**
     * GET PREFIX/v1/catalogs/{catalog}/namespaces/{namespaces}
     */
    public void getNamespace(Context ctx) {
      String catalog = catalog(ctx);
      String namespace = namespaces(ctx);
      if (mockStore.getDatabases(catalog).contains(namespace)){
        jsonResponse(ctx, GetNamespaceResponse.builder().withNamespace(Namespace.of(namespace)).build());
      } else {
        throw new NotFoundResponse("database not exists");
      }
    }

    /**
     * DELETE PREFIX/v1/catalogs/{catalog}/namespaces/{namespace}
     */
    public void dropNamespace(Context ctx) {
      String catalog = catalog(ctx);
      String namespace = namespaces(ctx);
      try {
        mockStore.dropDatabase(catalog, namespace);
      } catch (NoSuchObjectException e) {
        throw new NotFoundResponse("database not exits");
      }
    }

    /**
     * POST PREFIX/v1/catalogs/{catalog}/namespaces/{namespace}/properties
     */
    public void setNamespaceProperties(Context ctx) {
    }

    /**
     * GET PREFIX/v1/catalogs/{catalog}/namespaces/{namespace}/tables
     */
    public void listTablesInNamespace(Context ctx) {
    }

    /**
     * POST PREFIX/v1/catalogs/{catalog}/namespaces/{namespace}/tables
     */
    public void createTable(Context ctx) {
    }

    /**
     * GET PREFIX/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}
     */
    public void loadTable(Context ctx) {
    }

    /**
     * POST PREFIX/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}
     */
    public void commitTable(Context ctx) {
    }

    /**
     * DELETE PREFIX/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}
     */
    public void deleteTable(Context ctx) {
    }

    /**
     * HEAD PREFIX/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}
     */
    public void tableExists(Context ctx) {
    }

    /**
     * POST PREFIX/v1/catalogs/{catalog}/tables/rename
     */
    public void renameTable(Context ctx) {
      throw new UnsupportedOperationException("rename is not supported now.");
    }

    /**
     * POST PREFIX/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/metrics
     */
    public void metricReport(Context ctx) {
    }

    private void jsonResponse(Context ctx, RESTResponse rsp) {
      ctx.contentType(ContentType.APPLICATION_JSON)
          .result(jsonMapper.toJsonString(rsp));
    }

    private <T> T bodyAsClass(Context ctx, Class<T> clz) {
      return jsonMapper.fromJsonString(ctx.body(), clz);
    }

    private String catalog(Context ctx) {
      return ctx.pathParam("catalog");
    }

    private String namespaces(Context ctx) {
      return ctx.pathParam("namespaces");
    }
  }
}
