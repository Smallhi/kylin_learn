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

package org.apache.kylin.engine.spark;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.LookupMaterializeContext;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class SparkBatchCubingJobBuilder2 extends JobBuilderSupport {

    private static final Logger logger = LoggerFactory.getLogger(SparkBatchCubingJobBuilder2.class);

    private final ISparkInput.ISparkBatchCubingInputSide inputSide;
    private final ISparkOutput.ISparkBatchCubingOutputSide outputSide;

    public SparkBatchCubingJobBuilder2(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
        this.inputSide = SparkUtil.getBatchCubingInputSide(seg);
        this.outputSide = SparkUtil.getBatchCubingOutputSide(seg);
    }

    public CubingJob build() {
        logger.info("Spark new job to BUILD segment " + seg);

        final CubingJob result = CubingJob.createBuildJob(seg, submitter, config);
        final String jobId = result.getId();
        final String cuboidRootPath = getCuboidRootPath(jobId);

        // Phase 1: Create Flat Table & Materialize Hive View in Lookup Tables
        inputSide.addStepPhase1_CreateFlatTable(result);

        // Phase 2: Build Dictionary
        result.addTask(createFactDistinctColumnsStep(jobId));

        if (isEnableUHCDictStep()) {
            result.addTask(createBuildUHCDictStep(jobId));
        }

        result.addTask(createBuildDictionaryStep(jobId));
        result.addTask(createSaveStatisticsStep(jobId));

        // add materialize lookup tables if needed
        LookupMaterializeContext lookupMaterializeContext = addMaterializeLookupTableSteps(result);

        outputSide.addStepPhase2_BuildDictionary(result);

        // Phase 3: Build Cube
        addLayerCubingSteps(result, jobId, cuboidRootPath); // layer cubing, only selected algorithm will execute
        outputSide.addStepPhase3_BuildCube(result);

        // Phase 4: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterBuildStep(jobId, lookupMaterializeContext));
        inputSide.addStepPhase4_Cleanup(result);
        outputSide.addStepPhase4_Cleanup(result);

        return result;
    }


    protected void addLayerCubingSteps(final CubingJob result, final String jobId, final String cuboidRootPath) {
        final SparkExecutable sparkExecutable = new SparkExecutable();
        sparkExecutable.setClassName(SparkCubingByLayer.class.getName());
        configureSparkJob(seg, sparkExecutable, jobId, cuboidRootPath);
        result.addTask(sparkExecutable);
    }


    public void configureSparkJob(final CubeSegment seg, final SparkExecutable sparkExecutable,
            final String jobId, final String cuboidRootPath) {
        final IJoinedFlatTableDesc flatTableDesc = EngineFactory.getJoinedFlatTableDesc(seg);
        final String tablePath = JoinedFlatTable.getTableDir(flatTableDesc, getJobWorkingDir(jobId));
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_CUBE_NAME.getOpt(), seg.getRealization().getName());
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_SEGMENT_ID.getOpt(), seg.getUuid());
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_INPUT_TABLE.getOpt(),
                seg.getConfig().getHiveDatabaseForIntermediateTable() + "." + flatTableDesc.getTableName());
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_INPUT_PATH.getOpt(),
                tablePath);
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_META_URL.getOpt(),
                getSegmentMetadataUrl(seg.getConfig(), jobId));
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_OUTPUT_PATH.getOpt(), cuboidRootPath);
        sparkExecutable.setJobId(jobId);

        StringBuilder jars = new StringBuilder();

        StringUtil.appendWithSeparator(jars, seg.getConfig().getSparkAdditionalJars());
        sparkExecutable.setJars(jars.toString());
        sparkExecutable.setName(ExecutableConstants.STEP_NAME_BUILD_SPARK_CUBE);
    }

    public String getSegmentMetadataUrl(KylinConfig kylinConfig, String jobId) {
        Map<String, String> param = new HashMap<>();
        param.put("path", getDumpMetadataPath(jobId));
        return new StorageURL(kylinConfig.getMetadataUrl().getIdentifier(), "hdfs", param).toString();
    }
}
