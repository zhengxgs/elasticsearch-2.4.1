/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.bulk;

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class TransportBulkAction extends HandledTransportAction<BulkRequest, BulkResponse> {

    private final AutoCreateIndex autoCreateIndex;
    private final boolean allowIdGeneration;
    private final ClusterService clusterService;
    private final TransportShardBulkAction shardBulkAction;
    private final TransportCreateIndexAction createIndexAction;

    @Inject
    public TransportBulkAction(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                               TransportShardBulkAction shardBulkAction, TransportCreateIndexAction createIndexAction,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               AutoCreateIndex autoCreateIndex) {
        super(settings, BulkAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, BulkRequest.class);
        this.clusterService = clusterService;
        this.shardBulkAction = shardBulkAction;
        this.createIndexAction = createIndexAction;

        this.autoCreateIndex = autoCreateIndex;
        // TODO 是否自动生成ID，默认true, 通过参数 action.bulk.action.allow_id_generation 进行控制
        this.allowIdGeneration = this.settings.getAsBoolean("action.bulk.action.allow_id_generation", true);
    }

    /**
     * TODO 实际执行批量处理方法
     * @param bulkRequest
     * @param listener
     */
    @Override
    protected void doExecute(final BulkRequest bulkRequest, final ActionListener<BulkResponse> listener) {
        final long startTime = System.currentTimeMillis();
        final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());

        if (autoCreateIndex.needToCheck()) {
            // Keep track of all unique indices and all unique types per index for the create index requests:
            // 去重取一个，用于创建索引
            final Set<String> autoCreateIndices = new HashSet<>();
            for (ActionRequest request : bulkRequest.requests) {
                if (request instanceof DocumentRequest) {
                    DocumentRequest req = (DocumentRequest) request;
                    autoCreateIndices.add(req.index());
                } else {
                    throw new ElasticsearchException("Parsed unknown request in bulk actions: " + request.getClass().getSimpleName());
                }
            }
            final AtomicInteger counter = new AtomicInteger(autoCreateIndices.size());
            ClusterState state = clusterService.state();
            for (final String index : autoCreateIndices) {
                // TODO 判断是否需要创建索引，index可以是 Index Or Alias
                if (autoCreateIndex.shouldAutoCreate(index, state)) {
                    CreateIndexRequest createIndexRequest = new CreateIndexRequest(bulkRequest);
                    createIndexRequest.index(index);
                    createIndexRequest.cause("auto(bulk api)"); // 简单描述索引创建原因
                    createIndexRequest.masterNodeTimeout(bulkRequest.timeout());
                    createIndexAction.execute(createIndexRequest, new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse result) {
                            if (counter.decrementAndGet() == 0) {
                                try {
                                    executeBulk(bulkRequest, startTime, listener, responses);
                                } catch (Throwable t) {
                                    listener.onFailure(t);
                                }
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            if (!(ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException)) {
                                // fail all requests involving this index, if create didnt work
                                for (int i = 0; i < bulkRequest.requests.size(); i++) {
                                    ActionRequest request = bulkRequest.requests.get(i);
                                    if (request != null && setResponseFailureIfIndexMatches(responses, i, request, index, e)) {
                                        bulkRequest.requests.set(i, null);
                                    }
                                }
                            }
                            if (counter.decrementAndGet() == 0) {
                                try {
                                    executeBulk(bulkRequest, startTime, listener, responses);
                                } catch (Throwable t) {
                                    listener.onFailure(t);
                                }
                            }
                        }
                    });
                } else {
                    if (counter.decrementAndGet() == 0) {
                        executeBulk(bulkRequest, startTime, listener, responses);
                    }
                }
            }
        } else {
            executeBulk(bulkRequest, startTime, listener, responses);
        }
    }

    private boolean setResponseFailureIfIndexMatches(AtomicArray<BulkItemResponse> responses, int idx, ActionRequest request, String index, Throwable e) {
        if (request instanceof IndexRequest) {
            IndexRequest indexRequest = (IndexRequest) request;
            if (index.equals(indexRequest.index())) {
                responses.set(idx, new BulkItemResponse(idx, "index", new BulkItemResponse.Failure(indexRequest.index(), indexRequest.type(), indexRequest.id(), e)));
                return true;
            }
        } else if (request instanceof DeleteRequest) {
            DeleteRequest deleteRequest = (DeleteRequest) request;
            if (index.equals(deleteRequest.index())) {
                responses.set(idx, new BulkItemResponse(idx, "delete", new BulkItemResponse.Failure(deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), e)));
                return true;
            }
        } else if (request instanceof UpdateRequest) {
            UpdateRequest updateRequest = (UpdateRequest) request;
            if (index.equals(updateRequest.index())) {
                responses.set(idx, new BulkItemResponse(idx, "update", new BulkItemResponse.Failure(updateRequest.index(), updateRequest.type(), updateRequest.id(), e)));
                return true;
            }
        } else {
            throw new ElasticsearchException("Parsed unknown request in bulk actions: " + request.getClass().getSimpleName());
        }
        return false;
    }

    /**
     * This method executes the {@link BulkRequest} and calls the given listener once the request returns.
     * This method will not create any indices even if auto-create indices is enabled.
     *
     * @see #doExecute(BulkRequest, org.elasticsearch.action.ActionListener)
     */
    public void executeBulk(final BulkRequest bulkRequest, final ActionListener<BulkResponse> listener) {
        final long startTime = System.currentTimeMillis();
        executeBulk(bulkRequest, startTime, listener, new AtomicArray<BulkItemResponse>(bulkRequest.requests.size()));
    }

    private long buildTookInMillis(long startTime) {
        // protect ourselves against time going backwards
        return Math.max(1, System.currentTimeMillis() - startTime);
    }

    /**
     * 执行批量处理
     * @param bulkRequest
     * @param startTime
     * @param listener
     * @param responses
     */
    private void executeBulk(final BulkRequest bulkRequest, final long startTime, final ActionListener<BulkResponse> listener, final AtomicArray<BulkItemResponse> responses ) {
        final ClusterState clusterState = clusterService.state();
        // TODO use timeout to wait here if its blocked...

        // TODO 判断集群状态是否在block状态，如果是就抛出异常
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.WRITE);

        final ConcreteIndices concreteIndices = new ConcreteIndices(clusterState, indexNameExpressionResolver);
        MetaData metaData = clusterState.metaData();
        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            ActionRequest request = bulkRequest.requests.get(i);
            //the request can only be null because we set it to null in the previous step, so it gets ignored
            if (request == null) {
                // TODO 疑问：为什么有些调用入口要设置为null？
                continue;
            }
            DocumentRequest documentRequest = (DocumentRequest) request;
            if (addFailureIfIndexIsUnavailable(documentRequest, bulkRequest, responses, i, concreteIndices, metaData)) {
                continue;
            }
            // 解析index，根据不同的request进行不同的处理
            String concreteIndex = concreteIndices.resolveIfAbsent(documentRequest);
            if (request instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) request;
                MappingMetaData mappingMd = null;
                if (metaData.hasIndex(concreteIndex)) { // 判断是否有索引
                    // 获取mapping信息
                    mappingMd = metaData.index(concreteIndex).mappingOrDefault(indexRequest.type());
                }
                try {
                    // TODO 处理信息，获取routing啊等。。如果mapping信息里有的话
                    indexRequest.process(metaData, mappingMd, allowIdGeneration, concreteIndex);
                } catch (ElasticsearchParseException | RoutingMissingException e) {
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(concreteIndex, indexRequest.type(), indexRequest.id(), e);
                    BulkItemResponse bulkItemResponse = new BulkItemResponse(i, "index", failure);
                    responses.set(i, bulkItemResponse);
                    // make sure the request gets never processed again
                    bulkRequest.requests.set(i, null);
                }
            } else if (request instanceof DeleteRequest) {
                try {
                    // TODO 处理routing以及验证routing
					TransportDeleteAction.resolveAndValidateRouting(metaData, concreteIndex, (DeleteRequest) request);
                } catch(RoutingMissingException e) {
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(concreteIndex, documentRequest.type(), documentRequest.id(), e);
                    BulkItemResponse bulkItemResponse = new BulkItemResponse(i, "delete", failure);
                    responses.set(i, bulkItemResponse);
                    // make sure the request gets never processed again
                    bulkRequest.requests.set(i, null);
                }
            } else if (request instanceof UpdateRequest) {
                try {
                    // TODO 处理routing以及验证routing
                    TransportUpdateAction.resolveAndValidateRouting(metaData, concreteIndex, (UpdateRequest)request);
                } catch(RoutingMissingException e) {
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(concreteIndex, documentRequest.type(), documentRequest.id(), e);
                    BulkItemResponse bulkItemResponse = new BulkItemResponse(i, "update", failure);
                    responses.set(i, bulkItemResponse);
                    // make sure the request gets never processed again
                    bulkRequest.requests.set(i, null);
                }
            } else {
                throw new AssertionError("request type not supported: [" + request.getClass().getName() + "]");
            }
        }

        // first, go over all the requests and create a ShardId -> Operations mapping
        // TODO 根据不同的ShardId 进行分组
        Map<ShardId, List<BulkItemRequest>> requestsByShard = Maps.newHashMap();

        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            ActionRequest request = bulkRequest.requests.get(i);
            if (request instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) request;
                String concreteIndex = concreteIndices.getConcreteIndex(indexRequest.index());
                // TODO 确定request属于哪个分片，生产shardId
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, concreteIndex, indexRequest.type(), indexRequest.id(), indexRequest.routing()).shardId();
                List<BulkItemRequest> list = requestsByShard.get(shardId);
                if (list == null) {
                    list = new ArrayList<>();
                    requestsByShard.put(shardId, list);
                }
                list.add(new BulkItemRequest(i, request));
            } else if (request instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) request;
                String concreteIndex = concreteIndices.getConcreteIndex(deleteRequest.index());
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, concreteIndex, deleteRequest.type(), deleteRequest.id(), deleteRequest.routing()).shardId();
                List<BulkItemRequest> list = requestsByShard.get(shardId);
                if (list == null) {
                    list = new ArrayList<>();
                    requestsByShard.put(shardId, list);
                }
                list.add(new BulkItemRequest(i, request));
            } else if (request instanceof UpdateRequest) {
                UpdateRequest updateRequest = (UpdateRequest) request;
                String concreteIndex = concreteIndices.getConcreteIndex(updateRequest.index());
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, concreteIndex, updateRequest.type(), updateRequest.id(), updateRequest.routing()).shardId();
                List<BulkItemRequest> list = requestsByShard.get(shardId);
                if (list == null) {
                    list = new ArrayList<>();
                    requestsByShard.put(shardId, list);
                }
                list.add(new BulkItemRequest(i, request));
            }
        }

        if (requestsByShard.isEmpty()) {
            listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTime)));
            return;
        }

        final AtomicInteger counter = new AtomicInteger(requestsByShard.size());
        // TODO 遍历各个ShardId处理
        for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
            final ShardId shardId = entry.getKey();
            final List<BulkItemRequest> requests = entry.getValue();
            BulkShardRequest bulkShardRequest = new BulkShardRequest(bulkRequest, shardId, bulkRequest.refresh(), requests.toArray(new BulkItemRequest[requests.size()]));
            // TODO 设置一致性等级，默认为default等级
            bulkShardRequest.consistencyLevel(bulkRequest.consistencyLevel());
            // TODO 设置超时时间，默认1分钟
            bulkShardRequest.timeout(bulkRequest.timeout());

            // TODO 调用父类TransportAction的execute方法，最后调用到 TransportReplicationAction 的 doExecute(Task, Request, ActionListener<Response>) 方法
            shardBulkAction.execute(bulkShardRequest, new ActionListener<BulkShardResponse>() {
                @Override
                public void onResponse(BulkShardResponse bulkShardResponse) {
                    for (BulkItemResponse bulkItemResponse : bulkShardResponse.getResponses()) {
                        // we may have no response if item failed
                        if (bulkItemResponse.getResponse() != null) {
                            bulkItemResponse.getResponse().setShardInfo(bulkShardResponse.getShardInfo());
                        }
                        // TODO 成功的写入responses
                        responses.set(bulkItemResponse.getItemId(), bulkItemResponse);
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    // create failures for all relevant requests
                    for (BulkItemRequest request : requests) {
                        if (request.request() instanceof IndexRequest) {
                            IndexRequest indexRequest = (IndexRequest) request.request();
                            responses.set(request.id(), new BulkItemResponse(request.id(), indexRequest.opType().toString().toLowerCase(Locale.ENGLISH),
                                    new BulkItemResponse.Failure(concreteIndices.getConcreteIndex(indexRequest.index()), indexRequest.type(), indexRequest.id(), e)));
                        } else if (request.request() instanceof DeleteRequest) {
                            DeleteRequest deleteRequest = (DeleteRequest) request.request();
                            responses.set(request.id(), new BulkItemResponse(request.id(), "delete",
                                    new BulkItemResponse.Failure(concreteIndices.getConcreteIndex(deleteRequest.index()), deleteRequest.type(), deleteRequest.id(), e)));
                        } else if (request.request() instanceof UpdateRequest) {
                            UpdateRequest updateRequest = (UpdateRequest) request.request();
                            responses.set(request.id(), new BulkItemResponse(request.id(), "update",
                                    new BulkItemResponse.Failure(concreteIndices.getConcreteIndex(updateRequest.index()), updateRequest.type(), updateRequest.id(), e)));
                        }
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTime)));
                }
            });
        }
    }

    private boolean addFailureIfIndexIsUnavailable(DocumentRequest request, BulkRequest bulkRequest, AtomicArray<BulkItemResponse> responses, int idx,
                                              final ConcreteIndices concreteIndices,
                                              final MetaData metaData) {

        // TODO 先从jvm内存中取，
        String concreteIndex = concreteIndices.getConcreteIndex(request.index());
        Exception unavailableException = null;
        if (concreteIndex == null) {
            try {
                concreteIndex = concreteIndices.resolveIfAbsent(request);
            } catch (IndexClosedException | IndexNotFoundException ex) {
                // Fix for issue where bulk request references an index that
                // cannot be auto-created see issue #8125
                unavailableException = ex;
            }
        }
        if (unavailableException == null) {
            IndexMetaData indexMetaData = metaData.index(concreteIndex);
            if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                unavailableException = new IndexClosedException(new Index(metaData.index(request.index()).getIndex()));
            }
        }
        if (unavailableException != null) {
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(request.index(), request.type(), request.id(),
                    unavailableException);
            String operationType = "unknown";
            if (request instanceof IndexRequest) {
                operationType = "index";
            } else if (request instanceof DeleteRequest) {
                operationType = "delete";
            } else if (request instanceof UpdateRequest) {
                operationType = "update";
            }
            BulkItemResponse bulkItemResponse = new BulkItemResponse(idx, operationType, failure);
            responses.set(idx, bulkItemResponse);
            // make sure the request gets never processed again
            bulkRequest.requests.set(idx, null);
            return true;
        }
        return false;
    }


    private static class ConcreteIndices  {
        private final ClusterState state;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final Map<String, String> indices = new HashMap<>();

        ConcreteIndices(ClusterState state, IndexNameExpressionResolver indexNameExpressionResolver) {
            this.state = state;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
        }

        String getConcreteIndex(String indexOrAlias) {
            return indices.get(indexOrAlias);
        }

        String resolveIfAbsent(DocumentRequest request) {
            String concreteIndex = indices.get(request.index());
            if (concreteIndex == null) {
                concreteIndex = indexNameExpressionResolver.concreteSingleIndex(state, request);
                indices.put(request.index(), concreteIndex);
            }
            return concreteIndex;
        }
    }
}
