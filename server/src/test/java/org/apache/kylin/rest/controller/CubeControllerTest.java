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

package org.apache.kylin.rest.controller;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.request.CubeRequest;
import org.apache.kylin.rest.response.CubeInstanceResponse;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.rest.service.StreamingService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

/**
 * @author xduo
 */
public class CubeControllerTest extends ServiceTestBase {

    private CubeController cubeController;
    private CubeDescController cubeDescController;

    @Autowired
    @Qualifier("cubeMgmtService")
    CubeService cubeService;

    @Autowired
    @Qualifier("jobService")
    JobService jobService;

    @Autowired
    @Qualifier("streamingMgmtService")
    StreamingService streamingService;

    @Before
    public void setup() throws Exception {
        super.setup();

        cubeController = new CubeController();
        cubeController.setCubeService(cubeService);
        cubeController.setJobService(jobService);

        cubeDescController = new CubeDescController();
        cubeDescController.setCubeService(cubeService);

    }

    @Test
    public void testBasics() throws IOException {
        CubeDesc[] cubes = cubeDescController.getCube("test_kylin_cube_with_slr_ready");
        Assert.assertNotNull(cubes);
        Assert.assertNotNull(cubeController.getSql("test_kylin_cube_with_slr_ready", "20130331080000_20131212080000"));
        Assert.assertNotNull(cubeController.getCubes(null, null, null, 0, 5));

        CubeDesc cube = cubes[0];
        CubeDesc newCube = new CubeDesc();
        String newCubeName = cube.getName() + "_test_save";

        try {
            cubeController.deleteCube(newCubeName);
        } catch (Exception e) {
            // it may not exist, ignore the exception
        }

        newCube.setName(newCubeName);
        newCube.setModelName(cube.getModelName());
        newCube.setModel(cube.getModel());
        newCube.setDimensions(cube.getDimensions());
        newCube.setHbaseMapping(cube.getHbaseMapping());
        newCube.setMeasures(cube.getMeasures());
        newCube.setRowkey(cube.getRowkey());
        newCube.setAggregationGroups(cube.getAggregationGroups());

        newCube.getModel().setLastModified(0);

        ObjectMapper cubeDescMapper = new ObjectMapper();
        StringWriter cubeDescWriter = new StringWriter();
        cubeDescMapper.writeValue(cubeDescWriter, newCube);

        ObjectMapper modelDescMapper = new ObjectMapper();
        StringWriter modelDescWriter = new StringWriter();
        modelDescMapper.writeValue(modelDescWriter, newCube.getModel());

        CubeRequest cubeRequest = new CubeRequest();
        cubeRequest.setCubeDescData(cubeDescWriter.toString());
        cubeRequest.setCubeName(newCube.getName());
        cubeRequest = cubeController.saveCubeDesc(cubeRequest);

        List<String> notifyList = Lists.newArrayList();
        notifyList.add("john@example.com");
        cubeController.updateNotifyList(newCubeName, notifyList);

        List<CubeInstanceResponse> cubeInstances = cubeController.getCubes(newCubeName, cube.getModelName(), "default",
                1, 0);

        CubeInstance cubeInstance = cubeController.getCube(cubeInstances.get(0).getName());
        Assert.assertTrue(cubeInstance.getDescriptor().getNotifyList().contains("john@example.com"));
        Assert.assertTrue(cubeInstance.getCost() == 495);
        cubeController.deleteCube(newCubeName);
    }

    @Test(expected = InternalErrorException.class)
    public void testDeleteSegmentNew() throws IOException {
        String cubeName = "test_kylin_cube_with_slr_ready_3_segments";
        CubeDesc[] cubes = cubeDescController.getCube(cubeName);
        Assert.assertNotNull(cubes);

        cubeController.deleteSegment(cubeName, "20131212000000_20140112000000");
    }

    @Test(expected = NotFoundException.class)
    public void testDeleteSegmentNotExist() throws IOException {
        String cubeName = "test_kylin_cube_with_slr_ready_3_segments";
        CubeDesc[] cubes = cubeDescController.getCube(cubeName);
        Assert.assertNotNull(cubes);

        cubeController.deleteSegment(cubeName, "not_exist_segment");
    }

    @Test
    public void testDeleteSegmentFromHead() throws IOException {
        String cubeName = "test_kylin_cube_with_slr_ready_3_segments";
        CubeDesc[] cubes = cubeDescController.getCube(cubeName);
        Assert.assertNotNull(cubes);

        int segNumber = cubeService.getCubeManager().getCube(cubeName).getSegments().size();

        cubeController.deleteSegment(cubeName, "19691231160000_20131112000000");

        int newSegNumber = cubeService.getCubeManager().getCube(cubeName).getSegments().size();

        Assert.assertTrue(segNumber == newSegNumber + 1);
    }

    @Test
    public void testGetHoles() throws IOException {
        String cubeName = "test_kylin_cube_with_slr_ready_3_segments";
        CubeDesc[] cubes = cubeDescController.getCube(cubeName);
        Assert.assertNotNull(cubes);

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        List<CubeSegment> segments = cube.getSegments();

        final long dateEnd = segments.get(segments.size() - 1).getTSRange().end.v;

        final long ONEDAY = 24 * 60 * 60000;
        cubeService.getCubeManager().appendSegment(cube, new TSRange(dateEnd + ONEDAY, dateEnd + ONEDAY * 2));

        List<CubeSegment> holes = cubeController.getHoles(cubeName);

        Assert.assertTrue(holes.size() == 1);

        CubeSegment hole = holes.get(0);

        Assert.assertTrue(hole.getTSRange().equals(new TSRange(dateEnd, dateEnd + ONEDAY)));
    }

    @Test
    public void testGetCubes() {
        List<CubeInstanceResponse> cubes = cubeController.getCubes(null, null, null, 1, 0);
        Assert.assertTrue(cubes.size() == 1);
    }

    @Test
    public void testGetSql() {
        GeneralResponse response = cubeController.getSql("test_kylin_cube_with_slr_ready");
        String sql = response.getProperty("sql");
        CubeDesc cubeDesc = cubeDescController.getDesc("test_kylin_cube_with_slr_ready");

        for (DimensionDesc dimensionDesc : cubeDesc.getDimensions()) {
            if (dimensionDesc.getDerived() != null) {
                for (String derivedDimension : dimensionDesc.getDerived()) {
                    Assert.assertTrue(sql.contains(derivedDimension));
                }
            }
        }
    }

}
