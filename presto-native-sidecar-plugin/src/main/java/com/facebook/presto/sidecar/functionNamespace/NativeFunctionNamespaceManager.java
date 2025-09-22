/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sidecar.functionNamespace;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.functionNamespace.AbstractSqlInvokedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.ServingCatalog;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.AggregationFunctionMetadata;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlFunctionSupplier;
import com.facebook.presto.spi.function.SqlInvokedAggregationFunctionImplementation;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import jakarta.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.facebook.presto.builtin.tools.WorkerFunctionUtil.createSqlInvokedFunction;
import static com.facebook.presto.common.type.TypeSignatureUtils.resolveIntermediateType;
import static com.facebook.presto.spi.StandardErrorCode.DUPLICATE_FUNCTION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public class NativeFunctionNamespaceManager
        extends AbstractSqlInvokedFunctionNamespaceManager
{
    private static final Logger log = Logger.get(NativeFunctionNamespaceManager.class);
    private final Map<QualifiedObjectName, UserDefinedType> userDefinedTypes = new ConcurrentHashMap<>();
    private final Map<SqlFunctionHandle, AggregationFunctionImplementation> aggregationImplementationByHandle = new ConcurrentHashMap<>();
    private final FunctionDefinitionProvider functionDefinitionProvider;
    private final NodeManager nodeManager;
    private final Map<SqlFunctionId, SqlInvokedFunction> functions = new ConcurrentHashMap<>();
    private volatile Supplier<Map<SqlFunctionId, SqlInvokedFunction>> memoizedFunctionsSupplier;
    private final FunctionMetadataManager functionMetadataManager;
    private final LoadingCache<Signature, SqlFunctionSupplier> specializedFunctionKeyCache;

    @Inject
    public NativeFunctionNamespaceManager(
            @ServingCatalog String catalogName,
            SqlFunctionExecutors sqlFunctionExecutors,
            SqlInvokedFunctionNamespaceManagerConfig config,
            FunctionDefinitionProvider functionDefinitionProvider,
            NodeManager nodeManager,
            FunctionMetadataManager functionMetadataManager)
    {
        super(catalogName, sqlFunctionExecutors, config);
        this.functionDefinitionProvider = requireNonNull(functionDefinitionProvider, "functionDefinitionProvider is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.memoizedFunctionsSupplier = Suppliers.memoize(this::bootstrapNamespace);
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.specializedFunctionKeyCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(this::doGetSpecializedFunctionKey));
    }

    /**
     * Refreshes the function cache by invalidating the memoized supplier and clearing related caches.
     * This is useful when the sidecar function definitions change or when catalog configuration is updated.
     */
    public synchronized void refreshFunctionCache()
    {
        log.info("Refreshing function cache for catalog: %s", getCatalogName());
        
        // Invalidate the memoized supplier by creating a new one
        this.memoizedFunctionsSupplier = Suppliers.memoize(this::bootstrapNamespace);
        
        // Clear aggregation implementation cache as it depends on function definitions
        aggregationImplementationByHandle.clear();
        
        // Clear specialized function cache
        specializedFunctionKeyCache.invalidateAll();
        
        log.info("Function cache refreshed successfully");
    }

    private SqlFunctionSupplier doGetSpecializedFunctionKey(Signature signature)
    {
        return functionMetadataManager.getSpecializedFunctionKey(signature);
    }

    private synchronized Map<SqlFunctionId, SqlInvokedFunction> bootstrapNamespace()
    {
        log.info("Bootstrapping function namespace for catalog: %s", getCatalogName());
        
        functions.clear();
        
        try {
            UdfFunctionSignatureMap nativeFunctionSignatureMap = functionDefinitionProvider.getUdfDefinition(nodeManager);
            
            if (nativeFunctionSignatureMap == null) {
                log.warn("Function definition provider returned null for catalog: %s", getCatalogName());
                return ImmutableMap.of();
            }
            
            if (nativeFunctionSignatureMap.isEmpty()) {
                log.warn("Function definition provider returned empty map for catalog: %s", getCatalogName());
                return ImmutableMap.of();
            }
            
            log.info("Retrieved %d function groups from sidecar for catalog: %s", 
                    nativeFunctionSignatureMap.getUDFSignatureMap().size(), getCatalogName());
            
            populateNamespaceManager(nativeFunctionSignatureMap);
            
            checkArgument(!functions.isEmpty(), "functions map is empty after population for catalog: %s", getCatalogName());
            
            log.info("Successfully bootstrapped %d functions for catalog: %s", functions.size(), getCatalogName());
            return unmodifiableMap(functions);
        }
        catch (Exception e) {
            log.error(e, "Failed to bootstrap function namespace for catalog: %s", getCatalogName());
            throw new PrestoException(GENERIC_INTERNAL_ERROR, 
                    format("Failed to bootstrap function namespace for catalog '%s': %s", getCatalogName(), e.getMessage()), e);
        }
    }

    private synchronized void populateNamespaceManager(UdfFunctionSignatureMap udfFunctionSignatureMap)
    {
        Map<String, List<JsonBasedUdfFunctionMetadata>> udfSignatureMap = udfFunctionSignatureMap.getUDFSignatureMap();
        log.debug("Populating namespace manager with %d function groups for catalog: %s", 
                udfSignatureMap.size(), getCatalogName());
        
        udfSignatureMap.forEach((name, metaInfoList) -> {
            try {
                log.debug("Processing function group '%s' with %d variants for catalog: %s", 
                        name, metaInfoList.size(), getCatalogName());
                        
                List<SqlInvokedFunction> functions = metaInfoList.stream()
                        .map(metaInfo -> createSqlInvokedFunction(name, metaInfo, getCatalogName()))
                        .collect(toImmutableList());
                        
                functions.forEach(this::createFunction);
                
                log.debug("Successfully registered %d variants for function '%s' in catalog: %s", 
                        functions.size(), name, getCatalogName());
            }
            catch (Exception e) {
                log.error(e, "Failed to process function '%s' for catalog: %s", name, getCatalogName());
                throw new PrestoException(GENERIC_INTERNAL_ERROR, 
                        format("Failed to process function '%s' for catalog '%s': %s", name, getCatalogName(), e.getMessage()), e);
            }
        });
        
        log.info("Successfully populated %d total functions for catalog: %s", functions.size(), getCatalogName());
    }

    @Override
    public final AggregationFunctionImplementation getAggregateFunctionImplementation(FunctionHandle functionHandle, TypeManager typeManager)
    {
        checkCatalog(functionHandle);
        checkArgument(functionHandle instanceof SqlFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());

        SqlFunctionHandle sqlFunctionHandle = (SqlFunctionHandle) functionHandle;
        if (aggregationImplementationByHandle.containsKey(sqlFunctionHandle)) {
            return aggregationImplementationByHandle.get(sqlFunctionHandle);
        }
        if (functionHandle instanceof NativeFunctionHandle) {
            return processNativeFunctionHandle((NativeFunctionHandle) sqlFunctionHandle, typeManager);
        }
        else {
            return processSqlFunctionHandle(sqlFunctionHandle, typeManager);
        }
    }

    private AggregationFunctionImplementation processNativeFunctionHandle(NativeFunctionHandle nativeFunctionHandle, TypeManager typeManager)
    {
        Signature signature = nativeFunctionHandle.getSignature();
        SqlFunction function = getSqlFunctionFromSignature(signature);
        SqlInvokedFunction sqlFunction = (SqlInvokedFunction) function;

        checkArgument(
                sqlFunction.getAggregationMetadata().isPresent(),
                "Need aggregationMetadata to get aggregation function implementation");

        AggregationFunctionMetadata aggregationMetadata = sqlFunction.getAggregationMetadata().get();
        TypeSignature intermediateType = aggregationMetadata.getIntermediateType();
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                intermediateType, sqlFunction.getFunctionId().getArgumentTypes(), signature.getArgumentTypes());
        List<Type> parameters = signature.getArgumentTypes().stream().map(
                (typeManager::getType)).collect(toImmutableList());
        aggregationImplementationByHandle.put(
                nativeFunctionHandle,
                new SqlInvokedAggregationFunctionImplementation(
                        typeManager.getType(resolvedIntermediateType),
                        typeManager.getType(signature.getReturnType()),
                        aggregationMetadata.isOrderSensitive(),
                        parameters));
        return aggregationImplementationByHandle.get(nativeFunctionHandle);
    }

    private AggregationFunctionImplementation processSqlFunctionHandle(SqlFunctionHandle sqlFunctionHandle, TypeManager typeManager)
    {
        SqlFunctionId functionId = sqlFunctionHandle.getFunctionId();
        if (!memoizedFunctionsSupplier.get().containsKey(functionId)) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Function '%s' is missing from cache", functionId.getId()));
        }

        aggregationImplementationByHandle.put(
                sqlFunctionHandle,
                sqlInvokedFunctionToAggregationImplementation(memoizedFunctionsSupplier.get().get(functionId), typeManager));
        return aggregationImplementationByHandle.get(sqlFunctionHandle);
    }

    @Override
    protected Collection<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName functionName)
    {
        return memoizedFunctionsSupplier.get().values().stream()
                .filter(function -> function.getSignature().getName().equals(functionName))
                .collect(toImmutableList());
    }

    @Override
    protected UserDefinedType fetchUserDefinedTypeDirect(QualifiedObjectName typeName)
    {
        return userDefinedTypes.get(typeName);
    }

    @Override
    protected FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle)
    {
        if (functionHandle instanceof NativeFunctionHandle) {
            return getMetadataFromNativeFunctionHandle(functionHandle);
        }

        return fetchFunctionsDirect(functionHandle.getFunctionId().getFunctionName()).stream()
                .filter(function -> function.getRequiredFunctionHandle().equals(functionHandle))
                .map(this::sqlInvokedFunctionToMetadata).collect(onlyElement());
    }

    @Override
    protected ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle)
    {
        return fetchFunctionsDirect(functionHandle.getFunctionId().getFunctionName()).stream()
                .filter(function -> function.getRequiredFunctionHandle().equals(functionHandle))
                .map(this::sqlInvokedFunctionToImplementation)
                .collect(onlyElement());
    }

    @Override
    public synchronized void createFunction(SqlInvokedFunction function, boolean replace)
    {
        throw new PrestoException(NOT_SUPPORTED, "Create Function is not supported in NativeFunctionNamespaceManager");
    }

    @Override
    public void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
    {
        throw new PrestoException(NOT_SUPPORTED, "Alter Function is not supported in NativeFunctionNamespaceManager");
    }

    @Override
    public void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        throw new PrestoException(NOT_SUPPORTED, "Drop Function is not supported in NativeFunctionNamespaceManager");
    }

    @Override
    public Collection<SqlInvokedFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        return memoizedFunctionsSupplier.get().values();
    }

    @Override
    public void addUserDefinedType(UserDefinedType userDefinedType)
    {
        QualifiedObjectName name = userDefinedType.getUserDefinedTypeName();
        checkArgument(
                !userDefinedTypes.containsKey(name),
                "Parametric type %s already registered",
                name);
        userDefinedTypes.put(name, userDefinedType);
    }

    @Override
    public final FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        FunctionHandle functionHandle = super.getFunctionHandle(transactionHandle, signature);

        // only handle generic variadic signatures here , for normal signature we use the AbstractSqlInvokedFunctionNamespaceManager function handle.
        if (functionHandle == null) {
            return new NativeFunctionHandle(signature);
        }
        return functionHandle;
    }

    @VisibleForTesting
    public FunctionDefinitionProvider getFunctionDefinitionProvider()
    {
        return functionDefinitionProvider;
    }

    /**
     * Gets the current catalog name for this function namespace manager.
     * This is useful for troubleshooting catalog filtering issues.
     */
    @VisibleForTesting
    public String getCurrentCatalogName()
    {
        return getCatalogName();
    }

    /**
     * Gets the number of currently cached functions.
     * This is useful for troubleshooting and monitoring.
     */
    @VisibleForTesting
    public int getCachedFunctionCount()
    {
        return memoizedFunctionsSupplier.get().size();
    }

    private synchronized void createFunction(SqlInvokedFunction function)
    {
        checkFunctionLanguageSupported(function);
        SqlFunctionId functionId = function.getFunctionId();
        if (functions.containsKey(function.getFunctionId())) {
            throw new PrestoException(DUPLICATE_FUNCTION_ERROR, format("Function '%s' already exists", functionId.getId()));
        }

        functions.put(functionId, function.withVersion("1"));
    }

    private SqlFunction getSqlFunctionFromSignature(Signature signature)
    {
        try {
            return specializedFunctionKeyCache.getUnchecked(signature).getFunction();
        }
        catch (UncheckedExecutionException e) {
            throw convertToPrestoException(e, format("Error getting FunctionMetadata for signature: %s", signature));
        }
    }

    private FunctionMetadata getMetadataFromNativeFunctionHandle(SqlFunctionHandle functionHandle)
    {
        NativeFunctionHandle nativeFunctionHandle = (NativeFunctionHandle) functionHandle;
        Signature signature = nativeFunctionHandle.getSignature();
        SqlFunction function = getSqlFunctionFromSignature(signature);

        SqlInvokedFunction sqlFunction = (SqlInvokedFunction) function;
        return new FunctionMetadata(
                signature.getName(),
                signature.getArgumentTypes(),
                sqlFunction.getParameters().stream()
                        .map(Parameter::getName)
                        .collect(toImmutableList()),
                signature.getReturnType(),
                function.getSignature().getKind(),
                sqlFunction.getRoutineCharacteristics().getLanguage(),
                getFunctionImplementationType(sqlFunction),
                function.isDeterministic(),
                function.isCalledOnNullInput(),
                sqlFunction.getVersion(),
                function.getComplexTypeFunctionDescriptor());
    }

    private static PrestoException convertToPrestoException(UncheckedExecutionException exception, String failureMessage)
    {
        Throwable cause = exception.getCause();
        if (cause instanceof PrestoException) {
            return (PrestoException) cause;
        }
        return new PrestoException(GENERIC_INTERNAL_ERROR, failureMessage, cause);
    }
}
