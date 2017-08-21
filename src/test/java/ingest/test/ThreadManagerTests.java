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

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.Before;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import ingest.messaging.IngestThreadManager;
import ingest.messaging.IngestWorker;
import util.PiazzaLogger;

/**
 * Tests the Thread Manager
 * 
 * @author Patrick.Doody
 *
 */
public class ThreadManagerTests {
	@Mock
	private PiazzaLogger logger;
	@Mock
	private IngestWorker ingestWorker;
	@Mock
	private Consumer<String, String> consumer;
	@InjectMocks
	private IngestThreadManager manager;

	/**
	 * Test initialization
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		// Inject variables
		ReflectionTestUtils.setField(manager, "KAFKA_HOSTS", "localhost:9092");
		ReflectionTestUtils.setField(manager, "SPACE", "unit-test");
		ReflectionTestUtils.setField(manager, "KAFKA_GROUP", "job-unit-test");
	}

}
