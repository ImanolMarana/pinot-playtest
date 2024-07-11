/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.assignment.instance;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The instance tag/pool selector is responsible for selecting instances based on the tag and pool config.
 */
public class InstanceTagPoolSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceTagPoolSelector.class);

  private final InstanceTagPoolConfig _tagPoolConfig;
  private final String _tableNameWithType;
  private final boolean _minimizeDataMovement;
  private final InstancePartitions _existingInstancePartitions;

  public InstanceTagPoolSelector(InstanceTagPoolConfig tagPoolConfig, String tableNameWithType,
      boolean minimizeDataMovement, @Nullable InstancePartitions existingInstancePartitions) {
    _tagPoolConfig = tagPoolConfig;
    _tableNameWithType = tableNameWithType;
    _minimizeDataMovement = minimizeDataMovement && existingInstancePartitions != null;
    _existingInstancePartitions = existingInstancePartitions;
  }

  /**
   * Returns a map from pool to instance configs based on the tag and pool config for the given instance configs.
   */
  public Map<Integer, List<InstanceConfig>> selectInstances(List<InstanceConfig> instanceConfigs) {
    int tableNameHash = Math.abs(_tableNameWithType.hashCode());
    LOGGER.info("Starting instance tag/pool selection for table: {} with hash: {}", _tableNameWithType, tableNameHash);

    List<InstanceConfig> candidateInstanceConfigs = filterInstancesByTag(instanceConfigs);
    int numCandidateInstances = candidateInstanceConfigs.size();
    Preconditions.checkState(numCandidateInstances > 0, "No enabled instance has the tag: %s", _tagPoolConfig.getTag());
    LOGGER.info("{} enabled instances have the tag: {} for table: {}", numCandidateInstances, _tagPoolConfig.getTag(),
        _tableNameWithType);

    return _tagPoolConfig.isPoolBased() ? selectInstancesFromPools(candidateInstanceConfigs, tableNameHash)
        : selectInstancesWithoutPools(candidateInstanceConfigs);
  }

  private List<InstanceConfig> filterInstancesByTag(List<InstanceConfig> instanceConfigs) {
    String tag = _tagPoolConfig.getTag();
    List<InstanceConfig> candidateInstanceConfigs = new ArrayList<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      if (instanceConfig.getTags().contains(tag)) {
        candidateInstanceConfigs.add(instanceConfig);
      }
    }
    candidateInstanceConfigs.sort(Comparator.comparing(InstanceConfig::getInstanceName));
    return candidateInstanceConfigs;
  }

  private Map<Integer, List<InstanceConfig>> selectInstancesFromPools(List<InstanceConfig> candidateInstanceConfigs,
      int tableNameHash) {
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = buildPoolToInstanceConfigsMap(
        candidateInstanceConfigs);
    Preconditions.checkState(!poolToInstanceConfigsMap.isEmpty(),
        "No enabled instance has the pool configured for the tag: %s", _tagPoolConfig.getTag());

    Map<Integer, Integer> poolToNumInstancesMap = new TreeMap<>();
    for (Map.Entry<Integer, List<InstanceConfig>> entry : poolToInstanceConfigsMap.entrySet()) {
      poolToNumInstancesMap.put(entry.getKey(), entry.getValue().size());
    }
    LOGGER.info("Number instances for each pool: {} for table: {}", poolToNumInstancesMap, _tableNameWithType);

    Set<Integer> selectedPools = selectPools(poolToInstanceConfigsMap.keySet(), tableNameHash);

    // Keep the pools selected
    LOGGER.info("Selecting pools: {} for table: {}", selectedPools, _tableNameWithType);
    poolToInstanceConfigsMap.keySet().retainAll(selectedPools);
    return poolToInstanceConfigsMap;
  }

  private Map<Integer, List<InstanceConfig>> buildPoolToInstanceConfigsMap(
      List<InstanceConfig> candidateInstanceConfigs) {
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = new TreeMap<>();
    Map<String, Integer> instanceToPoolMap = new HashMap<>();
    for (InstanceConfig instanceConfig : candidateInstanceConfigs) {
      Map<String, String> poolMap = instanceConfig.getRecord().getMapField(InstanceUtils.POOL_KEY);
      if (poolMap != null && poolMap.containsKey(_tagPoolConfig.getTag())) {
        int pool = Integer.parseInt(poolMap.get(_tagPoolConfig.getTag()));
        poolToInstanceConfigsMap.computeIfAbsent(pool, k -> new ArrayList<>()).add(instanceConfig);
        instanceToPoolMap.put(instanceConfig.getInstanceName(), pool);
      }
    }
    return poolToInstanceConfigsMap;
  }

  private Set<Integer> selectPools(Set<Integer> pools, int tableNameHash) {
    List<Integer> poolsToSelect = _tagPoolConfig.getPools();
    if (!CollectionUtils.isEmpty(poolsToSelect)) {
      Preconditions.checkState(pools.containsAll(poolsToSelect), "Cannot find all instance pools configured: %s",
          poolsToSelect);
      return new TreeSet<>(poolsToSelect);
    }

    int numPools = pools.size();
    int numPoolsToSelect =
        _tagPoolConfig.getNumPools() > 0 ? Math.min(_tagPoolConfig.getNumPools(), numPools) : numPools;

    if (numPools == numPoolsToSelect) {
      LOGGER.info("Selecting all {} pools: {} for table: {}", numPools, pools, _tableNameWithType);
      return pools;
    }

    return selectPoolsBasedOnDistribution(pools, tableNameHash, numPoolsToSelect);
  }

  private Set<Integer> selectPoolsBasedOnDistribution(Set<Integer> pools, int tableNameHash, int numPoolsToSelect) {
    List<Integer> poolsInCluster = new ArrayList<>(pools);
    int startIndex = Math.abs(tableNameHash % pools.size());
    List<Integer> selectedPools = new ArrayList<>(numPoolsToSelect);

    if (_minimizeDataMovement) {
      assert _existingInstancePartitions != null;
      Map<Integer, Integer> poolToNumExistingInstancesMap = calculatePoolToExistingInstancesMap(poolsInCluster);
      List<Triple<Integer, Integer, Integer>> sortedPools = sortPoolsByExistingInstances(poolsInCluster, startIndex,
          poolToNumExistingInstancesMap);
      for (int i = 0; i < numPoolsToSelect; i++) {
        selectedPools.add(sortedPools.get(i).getLeft());
      }
    } else {
      for (int i = 0; i < numPoolsToSelect; i++) {
        selectedPools.add(poolsInCluster.get((startIndex + i) % poolsInCluster.size()));
      }
    }
    return new TreeSet<>(selectedPools);
  }

  private Map<Integer, Integer> calculatePoolToExistingInstancesMap(List<Integer> poolsInCluster) {
    Map<String, Integer> instanceToPoolMap = new HashMap<>();
    for (int pool : poolsInCluster) {
      for (InstanceConfig instanceConfig : _existingInstancePartitions.getInstances(0, 0)) {
        if (instanceConfig.getRecord().getMapField(InstanceUtils.POOL_KEY) != null
            && instanceConfig.getRecord().getMapField(InstanceUtils.POOL_KEY).containsValue(String.valueOf(pool))) {
          instanceToPoolMap.put(instanceConfig.getInstanceName(), pool);
        }
      }
    }

    Map<Integer, Integer> poolToNumExistingInstancesMap = new TreeMap<>();
    int existingNumPartitions = _existingInstancePartitions.getNumPartitions();
    int existingNumReplicaGroups = _existingInstancePartitions.getNumReplicaGroups();
    for (int partitionId = 0; partitionId < existingNumPartitions; partitionId++) {
      for (int replicaGroupId = 0; replicaGroupId < existingNumReplicaGroups; replicaGroupId++) {
        List<String> existingInstances = _existingInstancePartitions.getInstances(partitionId, replicaGroupId);
        for (String existingInstance : existingInstances) {
          Integer existingPool = instanceToPoolMap.get(existingInstance);
          if (existingPool != null) {
            poolToNumExistingInstancesMap.merge(existingPool, 1, Integer::sum);
          }
        }
      }
    }
    return poolToNumExistingInstancesMap;
  }

  private List<Triple<Integer, Integer, Integer>> sortPoolsByExistingInstances(List<Integer> poolsInCluster,
      int startIndex, Map<Integer, Integer> poolToNumExistingInstancesMap) {
    List<Triple<Integer, Integer, Integer>> triples = new ArrayList<>(poolsInCluster.size());
    for (int i = 0; i < poolsInCluster.size(); i++) {
      int pool = poolsInCluster.get((startIndex + i) % poolsInCluster.size());
      triples.add(Triple.of(pool, poolToNumExistingInstancesMap.getOrDefault(pool, 0), i));
    }
    triples.sort((o1, o2) -> {
      int result = Integer.compare(o2.getMiddle(), o1.getMiddle());
      return result != 0 ? result : Integer.compare(o1.getRight(), o2.getRight());
    });
    return triples;
  }

  private Map<Integer, List<InstanceConfig>> selectInstancesWithoutPools(
      List<InstanceConfig> candidateInstanceConfigs) {
    LOGGER.info("Selecting {} instances for table: {}", candidateInstanceConfigs.size(), _tableNameWithType);
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = new TreeMap<>();
    poolToInstanceConfigsMap.put(0, candidateInstanceConfigs);
    return poolToInstanceConfigsMap;
  }

//Refactoring end
  }
}
