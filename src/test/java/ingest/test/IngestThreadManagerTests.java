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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import com.fasterxml.jackson.databind.ObjectMapper;

import ingest.messaging.IngestThreadManager;
import ingest.messaging.IngestWorker;
import model.job.Job;
import model.job.JobProgress;
import model.job.type.AbortJob;
import model.job.type.IngestJob;
import model.logger.Severity;
import model.request.PiazzaJobRequest;
import model.status.StatusUpdate;
import util.PiazzaLogger;

public class IngestThreadManagerTests {
	@Mock
	private PiazzaLogger logger;
	@Mock
	private IngestWorker ingestWorker;
	@Spy
	private ObjectMapper mapper;
	@InjectMocks
	private IngestThreadManager ingestThreadManager;

	private ObjectMapper realMapper = new ObjectMapper();

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testProcessJob() throws Exception {
		// Mock a Job
		Job mockJob = new Job();
		mockJob.setJobId(UUID.randomUUID().toString());
		mockJob.setStatus(StatusUpdate.STATUS_RUNNING);
		mockJob.setProgress(new JobProgress(75));
		mockJob.setJobType(new IngestJob());
		String ingestJobRequest = realMapper.writeValueAsString(mockJob);
		// Test
		ingestThreadManager.processIngestJob(ingestJobRequest);
		// Verify
		Mockito.verify(ingestWorker).run(Mockito.isA(Job.class), Mockito.any());
	}

	/**
	 * Tests a bogus JSON request that is properly logged as an error
	 */
	@Test
	public void testProcessJobParseError() throws Exception {
		// Mock a Job
		String ingestJobRequest = "Bogus JSON";
		// Test
		ingestThreadManager.processIngestJob(ingestJobRequest);
		// Verify
		Mockito.verify(logger).log(Mockito.anyString(), Mockito.eq(Severity.ERROR));
	}

	/**
	 * Tests a cancellation request of a Job that is not present.
	 */
	@Test
	public void testProcessCancellationNotPresent() throws Exception {
		// Mock a Cancellation
		PiazzaJobRequest mockAbortRequest = new PiazzaJobRequest();
		mockAbortRequest.jobType = new AbortJob("123456");
		mockAbortRequest.createdBy = "A";
		String cancelJobRequest = realMapper.writeValueAsString(mockAbortRequest);
		// Test
		ingestThreadManager.processAbortJob(cancelJobRequest);
		// No errors
	}

	/**
	 * Tests a cancellation request that is improperly parsed
	 */
	@Test
	public void testProcessCancellationParseError() throws Exception {
		// Mock a Cancellation request that is improperly formatted
		PiazzaJobRequest mockAbortRequest = new PiazzaJobRequest();
		String cancelJobRequest = realMapper.writeValueAsString(mockAbortRequest);
		// Test
		ingestThreadManager.processAbortJob(cancelJobRequest);
		// Verify
		Mockito.verify(logger).log(Mockito.anyString(), Mockito.eq(Severity.ERROR));
	}

	@Test
	public void testGetRunningJobs() {
		List<String> jobs = ingestThreadManager.getRunningJobIds();
		assertNotNull(jobs);
		assertEquals(jobs.size(), 0);
	}
}
