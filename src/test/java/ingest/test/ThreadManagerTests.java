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

import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import ingest.messaging.IngestThreadManager;
import ingest.messaging.IngestWorker;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

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
		ReflectionTestUtils.setField(manager, "KAFKA_ADDRESS", "localhost:9092");
		ReflectionTestUtils.setField(manager, "SPACE", "unit-test");
		ReflectionTestUtils.setField(manager, "KAFKA_GROUP", "job-unit-test");
	}

	/**
	 * Test initialization of Kafka consuming
	 */
	@Test
	public void testInitialization() {
		// Mock
		Mockito.doNothing().when(consumer).subscribe(anyList());
		ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<String, String>(null);
		Mockito.when(consumer.poll(anyLong())).thenReturn(consumerRecords);

		// Test. Ensure no exceptions.
		manager.initialize();

		// No exceptions - then stop polling.
		manager.stopPolling();
	}
}
