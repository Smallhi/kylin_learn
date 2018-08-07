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

package org.apache.kylin.common.persistence;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ResourceTool {

    private static String[] includes = null;
    private static String[] excludes = null;
    private static final TreeSet<String> pathsSkipChildrenCheck = new TreeSet<>();

    private static final Logger logger = LoggerFactory.getLogger(ResourceTool.class);

    private static final Set<String> IMMUTABLE_PREFIX = Sets.newHashSet("/UUID");

    private static final List<String> SKIP_CHILDREN_CHECK_RESOURCE_ROOT = Lists
            .newArrayList(ResourceStore.EXECUTE_RESOURCE_ROOT, ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT);

    public static void main(String[] args) throws IOException {
        args = StringUtil.filterSystemArgs(args);

        if (args.length == 0) {
            System.out.println("Usage: ResourceTool list  RESOURCE_PATH");
            System.out.println("Usage: ResourceTool download  LOCAL_DIR");
            System.out.println("Usage: ResourceTool upload    LOCAL_DIR");
            System.out.println("Usage: ResourceTool reset");
            System.out.println("Usage: ResourceTool remove RESOURCE_PATH");
            System.out.println("Usage: ResourceTool cat RESOURCE_PATH");
            return;
        }

        String include = System.getProperty("include");
        if (include != null) {
            addIncludes(include.split("\\s*,\\s*"));
        }
        String exclude = System.getProperty("exclude");
        if (exclude != null) {
            addExcludes(exclude.split("\\s*,\\s*"));
        }

        addExcludes(IMMUTABLE_PREFIX.toArray(new String[IMMUTABLE_PREFIX.size()]));

        String cmd = args[0];
        switch (cmd) {
        case "reset":
            reset(args.length == 1 ? KylinConfig.getInstanceFromEnv() : KylinConfig.createInstanceFromUri(args[1]));
            break;
        case "list":
            list(KylinConfig.getInstanceFromEnv(), args[1]);
            break;
        case "download":
            copy(KylinConfig.getInstanceFromEnv(), KylinConfig.createInstanceFromUri(args[1]), true);
            break;
        case "fetch":
            copy(KylinConfig.getInstanceFromEnv(), KylinConfig.createInstanceFromUri(args[1]), args[2], true);
            break;
        case "upload":
            copy(KylinConfig.createInstanceFromUri(args[1]), KylinConfig.getInstanceFromEnv());
            break;
        case "remove":
            remove(KylinConfig.getInstanceFromEnv(), args[1]);
            break;
        case "cat":
            cat(KylinConfig.getInstanceFromEnv(), args[1]);
            break;
        default:
            System.out.println("Unknown cmd: " + cmd);
        }
    }

    public static String[] getIncludes() {
        return includes;
    }

    public static void addIncludes(String[] arg) {
        if (arg != null) {
            if (includes != null) {
                String[] nIncludes = new String[includes.length + arg.length];
                System.arraycopy(includes, 0, nIncludes, 0, includes.length);
                System.arraycopy(arg, 0, nIncludes, includes.length, arg.length);
                includes = nIncludes;
            } else {
                includes = arg;
            }
        }
    }

    public static String[] getExcludes() {
        return excludes;
    }

    public static void addExcludes(String[] arg) {
        if (arg != null) {
            if (excludes != null) {
                String[] nExcludes = new String[excludes.length + arg.length];
                System.arraycopy(excludes, 0, nExcludes, 0, excludes.length);
                System.arraycopy(arg, 0, nExcludes, excludes.length, arg.length);
                excludes = nExcludes;
            } else {
                excludes = arg;
            }
        }
    }

    public static String cat(KylinConfig config, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        InputStream is = store.getResource(path).inputStream;
        BufferedReader br = null;
        StringBuffer sb = new StringBuffer();
        String line;
        try {
            br = new BufferedReader(new InputStreamReader(is));
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                sb.append(line).append('\n');
            }
        } finally {
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(br);
        }
        return sb.toString();
    }

    public static NavigableSet<String> list(KylinConfig config, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        NavigableSet<String> result = store.listResources(path);
        System.out.println("" + result);
        return result;
    }

    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig, String path) throws IOException {
        copy(srcConfig, dstConfig, path, false);
    }

    //Do NOT invoke this method directly, unless you want to copy and possibly overwrite immutable resources such as UUID.
    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig, String path, boolean copyImmutableResource)
            throws IOException {
        ResourceStore src = ResourceStore.getStore(srcConfig);
        ResourceStore dst = ResourceStore.getStore(dstConfig);

        logger.info("Copy from {} to {}", src, dst);

        for (String resourceRoot : SKIP_CHILDREN_CHECK_RESOURCE_ROOT) {
            NavigableSet<String> all = src.listResourcesRecursively(resourceRoot);
            if (all != null) {
                pathsSkipChildrenCheck.addAll(src.listResourcesRecursively(resourceRoot));
            }
        }
        copyR(src, dst, path, copyImmutableResource);
    }

    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig, List<String> paths) throws IOException {
        copy(srcConfig, dstConfig, paths, false);
    }

    //Do NOT invoke this method directly, unless you want to copy and possibly overwrite immutable resources such as UUID.
    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig, List<String> paths,
            boolean copyImmutableResource) throws IOException {
        ResourceStore src = ResourceStore.getStore(srcConfig);
        ResourceStore dst = ResourceStore.getStore(dstConfig);

        logger.info("Copy from {} to {}", src, dst);

        for (String path : paths) {
            copyR(src, dst, path, copyImmutableResource);
        }
    }

    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig) throws IOException {
        copy(srcConfig, dstConfig, false);
    }

    //Do NOT invoke this method directly, unless you want to copy and possibly overwrite immutable resources such as UUID.
    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig, boolean copyImmutableResource)
            throws IOException {
        copy(srcConfig, dstConfig, "/", copyImmutableResource);
    }

    public static void copyR(ResourceStore src, ResourceStore dst, String path, boolean copyImmutableResource)
            throws IOException {

        if (!copyImmutableResource && IMMUTABLE_PREFIX.contains(path)) {
            return;
        }

        NavigableSet<String> children = null;

        if (!pathsSkipChildrenCheck.contains(path)) {
            children = src.listResources(path);
        }

        if (children == null) {
            // case of resource (not a folder)
            if (matchFilter(path)) {
                try {
                    RawResource res = src.getResource(path);
                    if (res != null) {
                        dst.putResource(path, res.inputStream, res.timestamp);
                        res.inputStream.close();
                    } else {
                        System.out.println("Resource not exist for " + path);
                    }
                } catch (Exception ex) {
                    System.err.println("Failed to open " + path);
                    logger.error(ex.getLocalizedMessage(), ex);
                }
            }
        } else {
            // case of folder
            for (String child : children)
                copyR(src, dst, child, copyImmutableResource);
        }

    }

    private static boolean matchFilter(String path) {
        if (includes != null) {
            boolean in = false;
            for (String include : includes) {
                in = in || path.startsWith(include);
            }
            if (!in)
                return false;
        }
        if (excludes != null) {
            for (String exclude : excludes) {
                if (path.startsWith(exclude))
                    return false;
            }
        }
        return true;
    }

    public static void reset(KylinConfig config) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        resetR(store, "/");
    }

    public static void resetR(ResourceStore store, String path) throws IOException {
        NavigableSet<String> children = store.listResources(path);
        if (children == null) { // path is a resource (not a folder)
            if (matchFilter(path)) {
                store.deleteResource(path);
            }
        } else {
            for (String child : children)
                resetR(store, child);
        }
    }

    private static void remove(KylinConfig config, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        resetR(store, path);
    }
}
