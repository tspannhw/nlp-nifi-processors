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
package com.mtnfog;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mtnfog.entity.Entity;
import com.mtnfog.entitydb.eql.filters.EqlFilters;

@Tags({ "query, entities, extraction" })
@CapabilityDescription("Provides entity filtering using the Entity Query Language.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class EqlProcessor extends AbstractProcessor {

	public static final PropertyDescriptor EQL_QUERY = new PropertyDescriptor.Builder()
			.name("EQL Query")
			.defaultValue("select * from entities")
			.description("The EQL query to filter the entities.")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final Relationship REL_MATCHES = new Relationship.Builder()
			.name("matches").description("matches").build();

	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure").description("failure").build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;
	
	private Gson gson;
	
	@Override
	protected void init(final ProcessorInitializationContext context) {
		
		descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(EQL_QUERY);
		descriptors = Collections.unmodifiableList(descriptors);

		relationships = new HashSet<Relationship>();
		relationships.add(REL_MATCHES);
		relationships.add(REL_FAILURE);
		relationships = Collections.unmodifiableSet(relationships);		
		
		gson = new Gson();
		
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext ctx,	final ProcessSession session) throws ProcessException {
		
		FlowFile flowFile = session.get();
		
		if (flowFile == null) {
			return;
		}
		
		final String eql = ctx.getProperty(EQL_QUERY).getValue();

		final AtomicReference<String> value = new AtomicReference<>();
		
		try {
						
			flowFile = session.write(flowFile, new StreamCallback() {
				
				@Override
				public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
					
					String input = IOUtils.toString(inputStream, Charset.forName("UTF-8"));
					
					Type listType = new TypeToken<ArrayList<Entity>>(){}.getType();
					List<Entity> entities = gson.fromJson(input, listType);
					
					Collection<Entity> filteredEntities = EqlFilters.filterEntities(entities, eql);

					final String json = gson.toJson(filteredEntities);
					value.set(json);
					IOUtils.write(json, outputStream, Charset.forName("UTF-8"));							
	
				}
				
			});
			
			session.transfer(flowFile, REL_MATCHES);
			
		} catch (Exception ex) {
			
			getLogger().error(String.format("Unable to filter entities with EQL: %s. Exception: %s", eql, ex.getMessage()), ex);
			session.transfer(flowFile, REL_FAILURE);
			
		}

	}
	
}
