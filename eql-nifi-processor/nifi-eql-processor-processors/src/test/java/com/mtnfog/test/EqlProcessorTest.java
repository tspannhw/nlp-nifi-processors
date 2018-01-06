/*
 * (C) Copyright 2017 Mountain Fog, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mtnfog.EqlProcessor;
import com.mtnfog.entity.Entity;

public class EqlProcessorTest {

    private TestRunner runner;

    @Before
    public void init() {
    	runner = TestRunners.newTestRunner(EqlProcessor.class);
    }

    @Test
    public void testOnTrigger1() throws IOException {
    	
    	Set<Entity> entities = new HashSet<Entity>();
    	entities.add(new Entity("George Washington"));
    	entities.add(new Entity("Abraham Lincoln"));
    	
    	Gson gson = new Gson();
    	
        InputStream content = new ByteArrayInputStream(gson.toJson(entities).getBytes());
        
        runner.setProperty(EqlProcessor.EQL_QUERY, "select * from entities where text = \"George Washington\"");
        runner.enqueue(content);
        runner.run(1);
        runner.assertQueueEmpty();
        
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(EqlProcessor.REL_MATCHES);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);

        String json = IOUtils.toString(runner.getContentAsByteArray(result), "UTF-8");
        Type listType = new TypeToken<HashSet<Entity>>(){}.getType();
        Set<Entity> filteredEntities = new Gson().fromJson(json, listType);
             
        assertEquals(1, filteredEntities.size());
        
    }
    
    @Test
    public void testOnTrigger2() throws IOException {
    	
    	Set<Entity> entities = new HashSet<Entity>();
    	entities.add(new Entity("George Washington"));
    	entities.add(new Entity("Abraham Lincoln"));
    	
    	Gson gson = new Gson();
    	
        InputStream content = new ByteArrayInputStream(gson.toJson(entities).getBytes());
        
        runner.setProperty(EqlProcessor.EQL_QUERY, "select * from entities where text = \"Thomas Jefferson\"");
        runner.enqueue(content);
        runner.run(1);
        runner.assertQueueEmpty();
        
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(EqlProcessor.REL_MATCHES);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);

        String json = IOUtils.toString(runner.getContentAsByteArray(result), "UTF-8");
        Type listType = new TypeToken<HashSet<Entity>>(){}.getType();
        Set<Entity> filteredEntities = new Gson().fromJson(json, listType);
             
        assertEquals(0, filteredEntities.size());
        
    }
    
}