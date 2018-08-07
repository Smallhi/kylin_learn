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

package org.apache.kylin.engine.mr;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CalculateStatsFromBaseCuboidJob;
import org.apache.kylin.engine.mr.steps.CreateDictionaryJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsJob;
import org.apache.kylin.engine.mr.steps.MergeDictionaryStep;
import org.apache.kylin.engine.mr.steps.MergeStatisticsStep;
import org.apache.kylin.engine.mr.steps.SaveStatisticsStep;
import org.apache.kylin.engine.mr.steps.UHCDictionaryJob;
import org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterBuildStep;
import org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterMergeStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;

import com.google.common.base.Preconditions;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * Hold reusable steps for builders.
 */
public class JobBuilderSupport {

    final protected JobEngineConfig config;
    final protected CubeSegment seg;
    final protected String submitter;

    final public static String LayeredCuboidFolderPrefix = "level_";

    final public static String PathNameCuboidBase = "base_cuboid";
    final public static String PathNameCuboidOld = "old";
    final public static String PathNameCuboidInMem = "in_memory";
    final public static Pattern JOB_NAME_PATTERN = Pattern.compile("kylin-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    public JobBuilderSupport(CubeSegment seg, String submitter) {
        Preconditions.checkNotNull(seg, "segment cannot be null");
        this.config = new JobEngineConfig(seg.getConfig());
        this.seg = seg;
        this.submitter = submitter;
    }

    public MapReduceExecutable createFactDistinctColumnsStep(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_FACT_DISTINCT_COLUMNS);
        result.setMapReduceJobClass(FactDistinctColumnsJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getFactDistinctColumnsPath(jobId));
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_STATS_OUTPUT, getStatisticsPath(jobId));
        appendExecCmdParameters(cmd, BatchConstants.ARG_STATS_SAMPLING_PERCENT, String.valueOf(config.getConfig().getCubingInMemSamplingPercent()));
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Fact_Distinct_Columns_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        result.setMapReduceParams(cmd.toString());
        result.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES);
        return result;
    }

    public MergeStatisticsStep createMergeStatisticsStep(CubeSegment seg, List<String> mergingSegmentIds, String mergedStatisticsFolder) {
        MergeStatisticsStep result = new MergeStatisticsStep();
        result.setName(ExecutableConstants.STEP_NAME_MERGE_STATISTICS);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setMergingSegmentIds(mergingSegmentIds, result.getParams());
        CubingExecutableUtil.setMergedStatisticsPath(mergedStatisticsFolder, result.getParams());

        return result;
    }

    public MapReduceExecutable createBuildUHCDictStep(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_BUILD_UHC_DICTIONARY);
        result.setMapReduceJobClass(UHCDictionaryJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getDictRootPath(jobId));
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getFactDistinctColumnsPath(jobId));
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Build_UHC_Dict" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        result.setMapReduceParams(cmd.toString());
        result.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES);
        return result;
    }

    public MapReduceExecutable createCalculateStatsFromBaseCuboid(String inputPath, String outputPath) {
        return createCalculateStatsFromBaseCuboid(inputPath, outputPath, CuboidModeEnum.CURRENT);
    }

    public MapReduceExecutable createCalculateStatsFromBaseCuboid(String inputPath, String outputPath,
            CuboidModeEnum cuboidMode) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_CALCULATE_STATS_FROM_BASE_CUBOID);
        result.setMapReduceJobClass(CalculateStatsFromBaseCuboidJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, inputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_STATS_SAMPLING_PERCENT,
                String.valueOf(config.getConfig().getCubingInMemSamplingPercent()));
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
                "Calculate_Stats_For_Segment_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBOID_MODE, cuboidMode.toString());

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public HadoopShellExecutable createBuildDictionaryStep(String jobId) {
        // base cuboid job
        HadoopShellExecutable buildDictionaryStep = new HadoopShellExecutable();
        buildDictionaryStep.setName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getFactDistinctColumnsPath(jobId));
        appendExecCmdParameters(cmd, BatchConstants.ARG_DICT_PATH, getDictRootPath(jobId));

        buildDictionaryStep.setJobParams(cmd.toString());
        buildDictionaryStep.setJobClass(CreateDictionaryJob.class);
        return buildDictionaryStep;
    }

    public UpdateCubeInfoAfterBuildStep createUpdateCubeInfoAfterBuildStep(String jobId, LookupMaterializeContext lookupMaterializeContext) {
        final UpdateCubeInfoAfterBuildStep result = new UpdateCubeInfoAfterBuildStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);
        result.getParams().put(BatchConstants.CFG_OUTPUT_PATH, getFactDistinctColumnsPath(jobId));
        if (lookupMaterializeContext != null) {
            result.getParams().put(BatchConstants.ARG_EXT_LOOKUP_SNAPSHOTS_INFO, lookupMaterializeContext.getAllLookupSnapshotsInString());
        }

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());

        return result;
    }

    public MergeDictionaryStep createMergeDictionaryStep(List<String> mergingSegmentIds) {
        MergeDictionaryStep result = new MergeDictionaryStep();
        result.setName(ExecutableConstants.STEP_NAME_MERGE_DICTIONARY);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setMergingSegmentIds(mergingSegmentIds, result.getParams());

        return result;
    }

    public UpdateCubeInfoAfterMergeStep createUpdateCubeInfoAfterMergeStep(List<String> mergingSegmentIds, String jobId) {
        UpdateCubeInfoAfterMergeStep result = new UpdateCubeInfoAfterMergeStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        CubingExecutableUtil.setMergingSegmentIds(mergingSegmentIds, result.getParams());

        return result;
    }


    public boolean isEnableUHCDictStep() {
        if (!config.getConfig().isBuildUHCDictWithMREnabled()) {
            return false;
        }

        List<TblColRef> uhcColumns = seg.getCubeDesc().getAllUHCColumns();
        if (uhcColumns.size() == 0) {
            return false;
        }

        return true;
    }

    public LookupMaterializeContext addMaterializeLookupTableSteps(final CubingJob result) {
        LookupMaterializeContext lookupMaterializeContext = new LookupMaterializeContext(result);
        CubeDesc cubeDesc = seg.getCubeDesc();
        List<String> allSnapshotTypes = cubeDesc.getAllExtLookupSnapshotTypes();
        if (allSnapshotTypes.isEmpty()) {
            return null;
        }
        for (String snapshotType : allSnapshotTypes) {
            ILookupMaterializer materializer = MRUtil.getExtLookupMaterializer(snapshotType);
            materializer.materializeLookupTablesForCube(lookupMaterializeContext, seg.getCubeInstance());
        }
        return lookupMaterializeContext;
    }


    public SaveStatisticsStep createSaveStatisticsStep(String jobId) {
        SaveStatisticsStep result = new SaveStatisticsStep();
        result.setName(ExecutableConstants.STEP_NAME_SAVE_STATISTICS);
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setStatisticsPath(getStatisticsPath(jobId), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        return result;
    }

    // ============================================================================

    public String getJobWorkingDir(String jobId) {
        return getJobWorkingDir(config, jobId);
    }

    public String getRealizationRootPath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getRealization().getName();
    }

    public String getCuboidRootPath(String jobId) {
        return getRealizationRootPath(jobId) + "/cuboid/";
    }

    public String getCuboidRootPath(CubeSegment seg) {
        return getCuboidRootPath(seg.getLastBuildJobID());
    }

    public void appendMapReduceParameters(StringBuilder buf) {
        appendMapReduceParameters(buf, JobEngineConfig.DEFAUL_JOB_CONF_SUFFIX);
    }

    public void appendMapReduceParameters(StringBuilder buf, String jobType) {
        try {
            String jobConf = config.getHadoopJobConfFilePath(jobType);
            if (jobConf != null && jobConf.length() > 0) {
                buf.append(" -conf ").append(jobConf);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getFactDistinctColumnsPath(String jobId) {
        return getRealizationRootPath(jobId) + "/fact_distinct_columns";
    }

    public String getStatisticsPath(String jobId) {
        return getRealizationRootPath(jobId) + "/fact_distinct_columns/" + BatchConstants.CFG_OUTPUT_STATISTICS;
    }

    public String getDictRootPath(String jobId) {
        return getRealizationRootPath(jobId) + "/dict";
    }

    public String getOptimizationRootPath(String jobId) {
        return getRealizationRootPath(jobId) + "/optimize";
    }

    public String getOptimizationStatisticsPath(String jobId) {
        return getOptimizationRootPath(jobId) + "/statistics";
    }

    public String getOptimizationCuboidPath(String jobId) {
        return getOptimizationRootPath(jobId) + "/cuboid/";
    }

    // ============================================================================
    // static methods also shared by other job flow participant
    // ----------------------------------------------------------------------------

    public static String getJobWorkingDir(JobEngineConfig conf, String jobId) {
        return getJobWorkingDir(conf.getHdfsWorkingDirectory(), jobId);
    }

    public static String getJobWorkingDir(String hdfsDir, String jobId) {
        if (!hdfsDir.endsWith("/")) {
            hdfsDir = hdfsDir + "/";
        }
        return hdfsDir + "kylin-" + jobId;
    }

    public static StringBuilder appendExecCmdParameters(StringBuilder buf, String paraName, String paraValue) {
        return buf.append(" -").append(paraName).append(" ").append(paraValue);
    }

    public static String getCuboidOutputPathsByLevel(String cuboidRootPath, int level) {
        if (level == 0) {
            return cuboidRootPath + LayeredCuboidFolderPrefix + PathNameCuboidBase;
        } else {
            return cuboidRootPath + LayeredCuboidFolderPrefix + level + "_cuboid";
        }
    }

    public static String getBaseCuboidPath(String cuboidRootPath) {
        return cuboidRootPath + PathNameCuboidBase;
    }

    public static String getInMemCuboidPath(String cuboidRootPath) {
        return cuboidRootPath + PathNameCuboidInMem;
    }

    public String getDumpMetadataPath(String jobId) {
        return getRealizationRootPath(jobId) + "/metadata";
    }


    public static String extractJobIDFromPath(String path) {
        Matcher matcher = JOB_NAME_PATTERN.matcher(path);
        // check the first occurrence
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            throw new IllegalStateException("Can not extract job ID from file path : " + path);
        }
    }
}
