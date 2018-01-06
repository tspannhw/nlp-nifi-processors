/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mtnfog.test;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mtnfog.IdylE3;
import com.mtnfog.entity.Entity;

public class IdylE3Test {

    private TestRunner runner;

    @Before
    public void init() {
    	runner = TestRunners.newTestRunner(IdylE3.class);
    }

    @Ignore("This is an integration test that connects to Idyl E3.")
    @Test
    public void testOnTrigger() throws IOException {
    	
        InputStream content = new ByteArrayInputStream("George Washington was president.".getBytes());
        
        runner.setProperty(IdylE3.IDYL_E3_HOST, "http://localhost:9000/");
        runner.setProperty(IdylE3.IDYL_E3_CONTEXT, "${uuid}");
        runner.enqueue(content);
        runner.run(1);
        runner.assertQueueEmpty();
        
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(IdylE3.REL_SUCCESS);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);

        String json = IOUtils.toString(runner.getContentAsByteArray(result), "UTF-8");
        Type listType = new TypeToken<HashSet<Entity>>(){}.getType();
        Set<Entity> entities = new Gson().fromJson(json, listType);
             
        assertEquals(1, entities.size());
        
    }
    
}