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

package org.apache.kylin.job.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.job.exception.PersistentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public class ExecutableDao {

    private static final Serializer<ExecutablePO> JOB_SERIALIZER = new JsonSerializer<ExecutablePO>(ExecutablePO.class);
    private static final Serializer<ExecutableOutputPO> JOB_OUTPUT_SERIALIZER = new JsonSerializer<ExecutableOutputPO>(ExecutableOutputPO.class);
    private static final Logger logger = LoggerFactory.getLogger(ExecutableDao.class);

    public static ExecutableDao getInstance(KylinConfig config) {
        return config.getManager(ExecutableDao.class);
    }

    // called by reflection
    static ExecutableDao newInstance(KylinConfig config) throws IOException {
        return new ExecutableDao(config);
    }

    // ============================================================================
    
    private ResourceStore store;

    private ExecutableDao(KylinConfig config) {
        logger.info("Using metadata url: " + config);
        this.store = ResourceStore.getStore(config);
    }

    private String pathOfJob(ExecutablePO job) {
        return pathOfJob(job.getUuid());
    }

    public static String pathOfJob(String uuid) {
        return ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + uuid;
    }

    public static String pathOfJobOutput(String uuid) {
        return ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + uuid;
    }

    private ExecutablePO readJobResource(String path) throws IOException {
        return store.getResource(path, ExecutablePO.class, JOB_SERIALIZER);
    }

    private long writeJobResource(String path, ExecutablePO job) throws IOException {
        return store.putResource(path, job, JOB_SERIALIZER);
    }

    private ExecutableOutputPO readJobOutputResource(String path) throws IOException {
        return store.getResource(path, ExecutableOutputPO.class, JOB_OUTPUT_SERIALIZER);
    }

    private long writeJobOutputResource(String path, ExecutableOutputPO output) throws IOException {
        return store.putResource(path, output, JOB_OUTPUT_SERIALIZER);
    }

    public List<ExecutableOutputPO> getJobOutputs() throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT, ExecutableOutputPO.class, JOB_OUTPUT_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public List<ExecutableOutputPO> getJobOutputs(long timeStart, long timeEndExclusive) throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT, timeStart, timeEndExclusive, ExecutableOutputPO.class, JOB_OUTPUT_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public List<ExecutablePO> getJobs() throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_RESOURCE_ROOT, ExecutablePO.class, JOB_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public List<ExecutablePO> getJobs(long timeStart, long timeEndExclusive) throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_RESOURCE_ROOT, timeStart, timeEndExclusive, ExecutablePO.class, JOB_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public List<String> getJobIds() throws PersistentException {
        try {
            NavigableSet<String> resources = store.listResources(ResourceStore.EXECUTE_RESOURCE_ROOT);
            if (resources == null) {
                return Collections.emptyList();
            }
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(resources.size());
            for (String path : resources) {
                result.add(path.substring(path.lastIndexOf("/") + 1));
            }
            return result;
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public ExecutablePO getJob(String uuid) throws PersistentException {
        try {
            return readJobResource(pathOfJob(uuid));
        } catch (IOException e) {
            logger.error("error get job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    public ExecutablePO addJob(ExecutablePO job) throws PersistentException {
        try {
            if (getJob(job.getUuid()) != null) {
                throw new IllegalArgumentException("job id:" + job.getUuid() + " already exists");
            }
            writeJobResource(pathOfJob(job), job);
            return job;
        } catch (IOException e) {
            logger.error("error save job:" + job.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public ExecutablePO updateJob(ExecutablePO job) throws PersistentException {
        try {
            if (getJob(job.getUuid()) == null) {
                throw new IllegalArgumentException("job id:" + job.getUuid() + " does not exist");
            }
            final long ts = writeJobResource(pathOfJob(job), job);
            job.setLastModified(ts);
            return job;
        } catch (IOException e) {
            logger.error("error update job:" + job.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public void deleteJob(String uuid) throws PersistentException {
        try {
            store.deleteResource(pathOfJob(uuid));
        } catch (IOException e) {
            logger.error("error delete job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    public ExecutableOutputPO getJobOutput(String uuid) throws PersistentException {
        try {
            ExecutableOutputPO result = readJobOutputResource(pathOfJobOutput(uuid));
            if (result == null) {
                result = new ExecutableOutputPO();
                result.setUuid(uuid);
                return result;
            }
            return result;
        } catch (IOException e) {
            logger.error("error get job output id:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    public void addJobOutput(ExecutableOutputPO output) throws PersistentException {
        try {
            output.setLastModified(0);
            writeJobOutputResource(pathOfJobOutput(output.getUuid()), output);
        } catch (IOException e) {
            logger.error("error update job output id:" + output.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public void updateJobOutput(ExecutableOutputPO output) throws PersistentException {
        try {
            final long ts = writeJobOutputResource(pathOfJobOutput(output.getUuid()), output);
            output.setLastModified(ts);
        } catch (IOException e) {
            logger.error("error update job output id:" + output.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public void deleteJobOutput(String uuid) throws PersistentException {
        try {
            store.deleteResource(pathOfJobOutput(uuid));
        } catch (IOException e) {
            logger.error("error delete job:" + uuid, e);
            throw new PersistentException(e);
        }
    }
}
