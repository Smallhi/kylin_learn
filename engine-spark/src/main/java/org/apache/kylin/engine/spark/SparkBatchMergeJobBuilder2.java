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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SparkBatchMergeJobBuilder2 extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(SparkBatchMergeJobBuilder2.class);

    private final ISparkOutput.ISparkBatchMergeOutputSide outputSide;
    private final ISparkInput.ISparkBatchMergeInputSide inputSide;

    public SparkBatchMergeJobBuilder2(CubeSegment mergeSegment, String submitter) {
        super(mergeSegment, submitter);
        this.outputSide = SparkUtil.getBatchMergeOutputSide2(seg);
        this.inputSide = SparkUtil.getBatchMergeInputSide(seg);
    }

    public CubingJob build() {
        logger.info("Spark_V2 new job to MERGE segment " + seg);

        final CubeSegment cubeSegment = seg;
        final CubingJob result = CubingJob.createMergeJob(cubeSegment, submitter, config);
        final String jobId = result.getId();

        final List<CubeSegment> mergingSegments = cubeSegment.getCubeInstance().getMergingSegments(cubeSegment);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge, target segment " + cubeSegment);
        final List<String> mergingSegmentIds = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingSegmentIds.add(merging.getUuid());
        }

        // Phase 1: Merge Dictionary
        inputSide.addStepPhase1_MergeDictionary(result);
        result.addTask(createMergeDictionaryStep(mergingSegmentIds));
        result.addTask(createMergeStatisticsStep(cubeSegment, mergingSegmentIds, getStatisticsPath(jobId)));
        outputSide.addStepPhase1_MergeDictionary(result);

        // merge cube
        result.addTask(createMergeCuboidDataStep(cubeSegment, mergingSegments, jobId));

        // Phase 2: Merge Cube Files
        outputSide.addStepPhase2_BuildCube(seg, mergingSegments, result);

        // Phase 3: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterMergeStep(mergingSegmentIds, jobId));
        outputSide.addStepPhase3_Cleanup(result);

        return result;
    }

    public SparkExecutable createMergeCuboidDataStep(CubeSegment seg, List<CubeSegment> mergingSegments, String jobID) {

        final List<String> mergingCuboidPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingCuboidPaths.add(getCuboidRootPath(merging));
        }
        String formattedPath = StringUtil.join(mergingCuboidPaths, ",");
        String outputPath = getCuboidRootPath(jobID);

        final SparkExecutable sparkExecutable = new SparkExecutable();
        sparkExecutable.setClassName(SparkCubingMerge.class.getName());
        sparkExecutable.setParam(SparkCubingMerge.OPTION_CUBE_NAME.getOpt(), seg.getRealization().getName());
        sparkExecutable.setParam(SparkCubingMerge.OPTION_SEGMENT_ID.getOpt(), seg.getUuid());
        sparkExecutable.setParam(SparkCubingMerge.OPTION_INPUT_PATH.getOpt(), formattedPath);
        sparkExecutable.setParam(SparkCubingMerge.OPTION_META_URL.getOpt(),
                getSegmentMetadataUrl(seg.getConfig(), jobID));
        sparkExecutable.setParam(SparkCubingMerge.OPTION_OUTPUT_PATH.getOpt(), outputPath);

        sparkExecutable.setJobId(jobID);
        sparkExecutable.setName(ExecutableConstants.STEP_NAME_MERGE_CUBOID);

        StringBuilder jars = new StringBuilder();

        StringUtil.appendWithSeparator(jars, seg.getConfig().getSparkAdditionalJars());
        sparkExecutable.setJars(jars.toString());

        return sparkExecutable;
    }

    public String getSegmentMetadataUrl(KylinConfig kylinConfig, String jobId) {
        Map<String, String> param = new HashMap<>();
        param.put("path", getDumpMetadataPath(jobId));
        return new StorageURL(kylinConfig.getMetadataUrl().getIdentifier(), "hdfs", param).toString();
    }
}
