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
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.util.ValidateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * ModelController is defined as Restful API entrance for UI.
 *
 * @author jiazhong
 */
@Controller //告诉Spring容器，这是一个控制器
@RequestMapping(value = "/models") //这个注解在类上，表明所有请求，这是个根路径
// 父类 BasicController 主要用于错误处理
public class ModelController extends BasicController {
    // 通过LoggerFactory 获取一个日志对象，用于几率日志
    private static final Logger logger = LoggerFactory.getLogger(ModelController.class);

    @Autowired
    // Bean注入时使用，可以写在字段或方法上，默认byType注入，若byName需使用注解@Qualifier，默认要求依赖的对象必须存在，若允许为空，设置requied 为false;
    // 实际上是在在配置文件中指明自动装配属性的。Spring容易自动装配是指通过反射创建实例
    @Qualifier("modelMgmtService") // byName 注入对象
    private ModelService modelService;

    @Autowired // byName 自动装配bean：projectService，用于项目管理的服务
    @Qualifier("projectService")
    private ProjectService projectService;


    //注解，值得是请求及服务映射，value说明是的请求的路径，即对外提供的API的URI，method说明的是请求方法，produces 说明是请求header要求的个好似
    @RequestMapping(value = "/validate/{modelName}", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody // 注解，说明相应body是非HTML类型
    public EnvelopeResponse<Boolean> validateModelName(@PathVariable String modelName) {
        //EnvelopeResponse 响应对象，包含code,data,msg三个变量，会被转成Json封装到响应对象中
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelService.isModelNameValidate(modelName), "");
    }

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public List<DataModelDesc> getModels(@RequestParam(value = "modelName", required = false) String modelName,
            @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset) {
        try {
            return modelService.getModels(modelName, projectName, limit, offset);
        } catch (IOException e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }
    }

    /**
     *
     * create model
     * @throws java.io.IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/json" })
    @ResponseBody
    // post请求，request body 是ModelRequest类型。ModelRequest是一个bean,用于表述用户提交的model 数据
    // @RequestBody 这个注解一般说明请求的数据是Json，会交给HandlerAdapter配置的HttpMessageConverters 来解析Post Data Body再绑定到对应的bean上
    // ModelRequest 中modelDescData方法存储了用户传入的model数据，这时是Json格式的
    public ModelRequest saveModelDesc(@RequestBody ModelRequest modelRequest) {
        //Update Model
        //先把Json反序列化成对象，通过 JsonUtil.readValue(json,bean)来反序列化的
        DataModelDesc modelDesc = deserializeDataModelDesc(modelRequest);
        if (modelDesc == null || StringUtils.isEmpty(modelDesc.getName())) {
            return modelRequest;
        }

        if (StringUtils.isEmpty(modelDesc.getName())) {
            logger.info("Model name should not be empty.");
            throw new BadRequestException("Model name should not be empty.");
        }
        if (!ValidateUtil.isAlphanumericUnderscore(modelDesc.getName())) {
            throw new BadRequestException(
                    String.format("Invalid model name %s, only letters, numbers and underscore " + "supported."),
                    modelDesc.getName());
        }

        try {
            //用户发过来的模型数据是没有唯一ID的，产生一个随机id
            modelDesc.setUuid(UUID.randomUUID().toString());
            // 如果用户传如的有项目名称，就取用户传入的，否则就ProjectInstance的默认项目名称default
            String projectName = (null == modelRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                    : modelRequest.getProject();
            // 调用modelService 的创建方法
            modelService.createModelDesc(projectName, modelDesc);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }

        // 为什么要返回modelRequest类型？
        modelRequest.setUuid(modelDesc.getUuid());
        modelRequest.setSuccessful(true);
        return modelRequest;
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public ModelRequest updateModelDesc(@RequestBody ModelRequest modelRequest) throws JsonProcessingException {
        DataModelDesc modelDesc = deserializeDataModelDesc(modelRequest);
        if (modelDesc == null) {
            return modelRequest;
        }
        try {
            modelDesc = modelService.updateModelAndDesc(modelRequest.getProject(), modelDesc);
        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException("You don't have right to update this model.");
        } catch (Exception e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }

        if (modelDesc.getError().isEmpty()) {
            modelRequest.setSuccessful(true);
        } else {
            logger.warn("Model " + modelDesc.getName() + " fail to update because " + modelDesc.getError());
            updateRequest(modelRequest, false, omitMessage(modelDesc.getError()));
        }
        String descData = JsonUtil.writeValueAsIndentString(modelDesc);
        modelRequest.setModelDescData(descData);
        return modelRequest;
    }

    @RequestMapping(value = "/{modelName}", method = { RequestMethod.DELETE }, produces = { "application/json" })
    @ResponseBody
    public void deleteModel(@PathVariable String modelName) {
        DataModelDesc desc = modelService.getDataModelManager().getDataModelDesc(modelName);
        if (null == desc) {
            throw new NotFoundException("Data Model with name " + modelName + " not found..");
        }
        try {
            modelService.dropModel(desc);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to delete model. " + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{modelName}/clone", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public ModelRequest cloneModel(@PathVariable String modelName, @RequestBody ModelRequest modelRequest) {
        // clone的步骤
        // 1. 获取数据模型元数据服务对象metaManager：DataModelManager
        // 2. 由元数据服务对象获取数据模型描述对象
        // 3. 数据模型描述对象getCopyOf的方法 【复制对象】，设置新的模型名称
        // 4. 由模型服务createModelDesc方法创建新的模型
        // 4. 由元数据服务对象的reloadDataModel 方法加载到缓存

        String project = StringUtils.trimToNull(modelRequest.getProject());

        // 后续要用到 DataModelManager 刷新内容，所以这里需要散步获取DataModelDesc

        DataModelManager metaManager = DataModelManager.getInstance(KylinConfig.getInstanceFromEnv());
        DataModelDesc modelDesc = metaManager.getDataModelDesc(modelName);

        String newModelName = modelRequest.getModelName();

        if (null == project) {
            throw new BadRequestException("Project name should not be empty.");
        }

        if (modelDesc == null || StringUtils.isEmpty(modelName)) {
            throw new NotFoundException("Model does not exist.");
        }

        if (!project.equals(modelDesc.getProject())) {
            throw new BadRequestException("Cloning model across projects is not supported.");
        }

        if (StringUtils.isEmpty(newModelName)) {
            throw new BadRequestException("New model name should not be empty.");
        }
        if (!ValidateUtil.isAlphanumericUnderscore(newModelName)) {
            throw new BadRequestException(String
                    .format("Invalid model name %s, only letters, numbers and underscore supported.", newModelName));
        }

        DataModelDesc newModelDesc = DataModelDesc.getCopyOf(modelDesc);
        newModelDesc.setName(newModelName);
        try {
            newModelDesc = modelService.createModelDesc(project, newModelDesc);

            //reload avoid shallow
            metaManager.reloadDataModel(newModelName);
        } catch (IOException e) {
            throw new InternalErrorException("failed to clone DataModelDesc", e);
        }

        modelRequest.setUuid(newModelDesc.getUuid());
        modelRequest.setSuccessful(true);
        return modelRequest;
    }

    private DataModelDesc deserializeDataModelDesc(ModelRequest modelRequest) {
        DataModelDesc desc = null;
        try {
            logger.debug("Saving MODEL " + modelRequest.getModelDescData());
            desc = JsonUtil.readValue(modelRequest.getModelDescData(), DataModelDesc.class);
        } catch (JsonParseException e) {
            logger.error("The data model definition is not valid.", e);
            updateRequest(modelRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The data model definition is not valid.", e);
            updateRequest(modelRequest, false, e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return desc;
    }

    private void updateRequest(ModelRequest request, boolean success, String message) {
        request.setModelDescData("");
        request.setSuccessful(success);
        request.setMessage(message);
    }

    public void setModelService(ModelService modelService) {
        this.modelService = modelService;
    }

    /**
     * @param errors
     * @return
     */
    private String omitMessage(List<String> errors) {
        StringBuffer buffer = new StringBuffer();
        for (Iterator<String> iterator = errors.iterator(); iterator.hasNext();) {
            String string = (String) iterator.next();
            buffer.append(string);
            buffer.append("\n");
        }
        return buffer.toString();
    }

}
