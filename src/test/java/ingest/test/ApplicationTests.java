/**
 * Copyright 2016, RadiantBlue Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package ingest.test;

import static org.junit.Assert.assertNotNull;

import java.util.concurrent.Executor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import ingest.Application;

public class ApplicationTests {
	@InjectMocks
	private Application application;

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		ReflectionTestUtils.setField(application, "threadCountSize", 5);
		ReflectionTestUtils.setField(application, "threadCountLimit", 5);
	}

	@Test
	public void testRestTemplateCreation() {
		RestTemplate restTemplate = application.restTemplate();
		assertNotNull(restTemplate);
	}

	@Test
	public void testAsyncExecutor() {
		Executor executor = application.getAsyncExecutor();
		assertNotNull(executor);
	}
}
