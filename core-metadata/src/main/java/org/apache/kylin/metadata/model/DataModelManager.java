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

package org.apache.kylin.metadata.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public class DataModelManager {

    private static final Logger logger = LoggerFactory.getLogger(DataModelManager.class);

    public static DataModelManager getInstance(KylinConfig config) {
        return config.getManager(DataModelManager.class);
    }

    // called by reflection
    static DataModelManager newInstance(KylinConfig conf) {
        try {
            //如果配置文件有，就去配置文件的，否则默认取DataModelManager.class.getName()
            //通过反射创建类的实例
            String cls = StringUtil.noBlank(conf.getDataModelManagerImpl(), DataModelManager.class.getName());
            Class<? extends DataModelManager> clz = ClassUtil.forName(cls, DataModelManager.class);
            return clz.getConstructor(KylinConfig.class).newInstance(conf);
        } catch (Exception e) {
            throw new RuntimeException("Failed to init DataModelManager from " + conf, e);
        }
    }

    // ============================================================================

    private KylinConfig config;

    // name => DataModelDesc

    // crud 的父类包含一个熟悉BroadCast， 所有这个缓存是可以广播和同步的
    private CaseInsensitiveStringCache<DataModelDesc> dataModelDescMap;
    private CachedCrudAssist<DataModelDesc> crud;

    // protects concurrent operations around the cached map, to avoid for example
    // writing an entity in the middle of reloading it (dirty read)
    // 自动读写锁目的只是锁住读写，数据还在crud里
    private AutoReadWriteLock modelMapLock = new AutoReadWriteLock();

    public DataModelManager(KylinConfig config) throws IOException {
        // 封装构造方法为init
        init(config);
    }

    protected void init(KylinConfig cfg) throws IOException {
        this.config = cfg;
        this.dataModelDescMap = new CaseInsensitiveStringCache<>(config, "data_model");
        this.crud = new CachedCrudAssist<DataModelDesc>(getStore(), ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT,
                getDataModelImplClass(), dataModelDescMap) {
            @Override
            protected DataModelDesc initEntityAfterReload(DataModelDesc model, String resourceName) {
                String prj = ProjectManager.getInstance(config).getProjectOfModel(model.getName()).getName();
                if (!model.isDraft()) {
                    model.init(config, getAllTablesMap(prj), getModels(prj), true);
                }
                return model;
            }
        };

        // touch lower level metadata before registering model listener
        TableMetadataManager.getInstance(config);
        // curd加载数据
        crud.reloadAll();
        //通知Listener，getInstance会创建一个通告实例，创建的过程就会启动线程并发事件分发出去，实际上应该由Listeners 实现处理完成后通知处理事件的，
        Broadcaster.getInstance(config).registerListener(new DataModelSyncListener(), "data_model");
    }

    private class DataModelSyncListener extends Broadcaster.Listener {

        //用户的缓存不需要在每台机器都存，所以直接load到本机缓存
        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            //clean up the current project's table desc
            TableMetadataManager.getInstance(config).resetProjectSpecificTableDesc(project);

            try (AutoLock lock = modelMapLock.lockForWrite()) {
                for (String model : ProjectManager.getInstance(config).getProject(project).getModels()) {
                    crud.reloadQuietly(model);
                }
            }
            // 这个事件不需要通知
            //broadcaster.notifyListener();
            /**
             *
             */
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            try (AutoLock lock = modelMapLock.lockForWrite()) {
                if (event == Event.DROP)
                    dataModelDescMap.removeLocal(cacheKey);
                else
                    crud.reloadQuietly(cacheKey);
            }
        // 通知我完成了。。。
            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjectsByModel(cacheKey)) {
                broadcaster.notifyProjectSchemaUpdate(prj.getName());
            }
        }
    }

    private Class<DataModelDesc> getDataModelImplClass() {
        try {
            String cls = StringUtil.noBlank(config.getDataModelImpl(), DataModelDesc.class.getName());
            Class<? extends DataModelDesc> clz = ClassUtil.forName(cls, DataModelDesc.class);
            return (Class<DataModelDesc>) clz;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    // for test mostly
    public Serializer<DataModelDesc> getDataModelSerializer() {
        return crud.getSerializer();
    }

    public List<DataModelDesc> listDataModels() {
        try (AutoLock lock = modelMapLock.lockForRead()) {
            return Lists.newArrayList(dataModelDescMap.values());
        }
    }

    public DataModelDesc getDataModelDesc(String name) {
        try (AutoLock lock = modelMapLock.lockForRead()) {
            return dataModelDescMap.get(name);
        }
    }

    public List<DataModelDesc> getModels() {
        try (AutoLock lock = modelMapLock.lockForRead()) {
            return new ArrayList<>(dataModelDescMap.values());
        }
    }

    public List<DataModelDesc> getModels(String projectName) {
        try (AutoLock lock = modelMapLock.lockForRead()) {
            ProjectInstance projectInstance = ProjectManager.getInstance(config).getProject(projectName);
            ArrayList<DataModelDesc> ret = new ArrayList<>();

            if (projectInstance != null && projectInstance.getModels() != null) {
                for (String modelName : projectInstance.getModels()) {
                    DataModelDesc model = getDataModelDesc(modelName);
                    if (null != model) {
                        ret.add(model);
                    } else {
                        logger.info("Model " + modelName + " is missing or unloaded yet");
                    }
                }
            }

            return ret;
        }
    }

    // within a project, find models that use the specified table
    public List<String> getModelsUsingTable(TableDesc table, String project) throws IOException {
        try (AutoLock lock = modelMapLock.lockForRead()) {
            List<String> models = new ArrayList<>();
            for (DataModelDesc modelDesc : getModels(project)) {
                if (modelDesc.containsTable(table))
                    models.add(modelDesc.getName());
            }
            return models;
        }
    }

    public boolean isTableInAnyModel(TableDesc table) {
        try (AutoLock lock = modelMapLock.lockForRead()) {
            for (DataModelDesc modelDesc : getModels()) {
                if (modelDesc.containsTable(table))
                    return true;
            }
            return false;
        }
    }

    public DataModelDesc reloadDataModel(String modelName) {
        try (AutoLock lock = modelMapLock.lockForWrite()) {
            return crud.reload(modelName);
        }
    }

    public DataModelDesc dropModel(DataModelDesc desc) throws IOException {
        try (AutoLock lock = modelMapLock.lockForWrite()) {
            crud.delete(desc);
            // delete model from project
            ProjectManager.getInstance(config).removeModelFromProjects(desc.getName());
            return desc;
        }
    }

    public DataModelDesc createDataModelDesc(DataModelDesc desc, String projectName, String owner) throws IOException {
        try (AutoLock lock = modelMapLock.lockForWrite()) {
            String name = desc.getName();
            if (dataModelDescMap.containsKey(name))
                throw new IllegalArgumentException("DataModelDesc '" + name + "' already exists");

            ProjectManager prjMgr = ProjectManager.getInstance(config);
            ProjectInstance prj = prjMgr.getProject(projectName);
            if (prj.containsModel(name))
                throw new IllegalStateException("project " + projectName + " already contains model " + name);

            try {
                // Temporarily register model under project, because we want to 
                // update project formally after model is saved.
                prj.getModels().add(name);

                desc.setOwner(owner);
                logger.info("Saving Model {} to Project {} with {} as owner", desc.getName(), projectName, owner);
                desc = saveDataModelDesc(desc);

            } finally {
                prj.getModels().remove(name);
            }

            // now that model is saved, update project formally
            prjMgr.addModelToProject(name, projectName);

            return desc;
        }
    }

    public DataModelDesc updateDataModelDesc(DataModelDesc desc) throws IOException {
        try (AutoLock lock = modelMapLock.lockForWrite()) {
            String name = desc.getName();
            if (!dataModelDescMap.containsKey(name)) {
                throw new IllegalArgumentException("DataModelDesc '" + name + "' does not exist.");
            }

            return saveDataModelDesc(desc);
        }
    }

    private DataModelDesc saveDataModelDesc(DataModelDesc dataModelDesc) throws IOException {

        String prj = ProjectManager.getInstance(config).getProjectOfModel(dataModelDesc.getName()).getName();

        if (!dataModelDesc.isDraft())
            dataModelDesc.init(config, this.getAllTablesMap(prj), getModels(prj), false);

        crud.save(dataModelDesc);

        return dataModelDesc;
    }

    private Map<String, TableDesc> getAllTablesMap(String prj) {
        return TableMetadataManager.getInstance(config).getAllTablesMap(prj);
    }

}
