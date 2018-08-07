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

package org.apache.kylin.rest.service;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.acl.TableACLManager;
import org.apache.kylin.metadata.badquery.BadQueryHistoryManager;
import org.apache.kylin.metadata.draft.DraftManager;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.streaming.StreamingManager;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.storage.hybrid.HybridManager;

public abstract class BasicService {


    // 所有Manager都是工厂模式，通过getInstance 获取一个实例,这是一个抽象类，Manager是通过get方法给子类提供，没有实现抽象方法，子类可以直接
    // 调用父类的方法
    // Kylin 基本的服务的Manger有
    // 1. TableMetadataManager
    // 2. DataModelManager
    // 3. CubeManager
    // 4. StreamingManager
    // 5. KafkaConfigManager
    // 6. CubeDescManager
    // 7. ProjectManager
    // 8. HybridManager
    // 9. ExecutableManager
    //10. BadQueryHistoryManager
    //11. DraftManager
    //12. TableACLManager
    //13. MetricsManager
    //KylinConfig是读取配置文件，里面配置了Kylin的所有参数，通过getInstanceFromEnv是从环境变量中读取配置文件，并加载为KylinConfig对象
    public KylinConfig getConfig() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (kylinConfig == null) {
            throw new IllegalArgumentException("Failed to load kylin config instance");
        }

        return kylinConfig;
    }

    public TableMetadataManager getTableManager() {
        return TableMetadataManager.getInstance(getConfig());
    }
    
    public DataModelManager getDataModelManager() {
        return DataModelManager.getInstance(getConfig());
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(getConfig());
    }

    public StreamingManager getStreamingManager() {
        return StreamingManager.getInstance(getConfig());
    }

    public KafkaConfigManager getKafkaManager() throws IOException {
        return KafkaConfigManager.getInstance(getConfig());
    }

    public CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getConfig());
    }

    public ProjectManager getProjectManager() {
        return ProjectManager.getInstance(getConfig());
    }

    public HybridManager getHybridManager() {
        return HybridManager.getInstance(getConfig());
    }

    public ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(getConfig());
    }

    public BadQueryHistoryManager getBadQueryHistoryManager() {
        return BadQueryHistoryManager.getInstance(getConfig());
    }
    
    public DraftManager getDraftManager() {
        return DraftManager.getInstance(getConfig());
    }

    public TableACLManager getTableACLManager() {
        return TableACLManager.getInstance(getConfig());
    }

    public MetricsManager getMetricsManager() {
        return MetricsManager.getInstance();
    }
}
