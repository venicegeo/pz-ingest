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
package ingest.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import messaging.job.WorkerCallback;
import model.job.Job;
import model.job.type.AbortJob;
import model.logger.Severity;
import model.request.PiazzaJobRequest;
import util.PiazzaLogger;

/**
 * Main listener class for Ingest Jobs. Handles an incoming Ingest Job request by indexing metadata, storing files, and
 * updating appropriate database tables. This class manages the Thread Pool of running Ingest Jobs.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class IngestThreadManager {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private IngestWorker ingestWorker;

	@Value("${SPACE}")
	private String SPACE;

	private Map<String, Future<?>> runningJobs = new HashMap<String, Future<?>>();
	private static final Logger LOG = LoggerFactory.getLogger(IngestThreadManager.class);
	private ObjectMapper mapper = new ObjectMapper();

	/**
	 * Processes a message for Ingesting Data
	 * 
	 * @param ingestJobRequest
	 *            The PiazzaJobRequest with the Ingest Job information
	 */
	@RabbitListener(queues = "IngestJob-${SPACE}")
	public void processIngestJob(String ingestJobRequest) {
		try {
			// Callback that will be invoked when a Worker completes. This will
			// remove the Job Id from the running Jobs list.
			WorkerCallback callback = (String jobId) -> runningJobs.remove(jobId);
			// Get the Job Model
			Job job = mapper.readValue(ingestJobRequest, Job.class);
			// Process the work
			Future<?> workerFuture = ingestWorker.run(job, callback);
			// Keep track of this running Job's ID
			runningJobs.put(job.getJobId(), workerFuture);
		} catch (IOException exception) {
			String error = String.format("Error Reading Ingest Job Message from Queue %s", exception.getMessage());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR);
		}
	}

	/**
	 * Process a message for cancelling a Job. If this instance of the Loader contains this job, it will be terminated.
	 * 
	 * @param abortJobRequest
	 *            The information regarding the job to abort
	 */
	@RabbitListener(queues = "Abort-Job-${SPACE}")
	public void processAbortJob(String abortJobRequest) {
		String jobId = null;
		try {
			PiazzaJobRequest request = mapper.readValue(abortJobRequest, PiazzaJobRequest.class);
			jobId = ((AbortJob) request.jobType).getJobId();
		} catch (Exception exception) {
			String error = String.format("Error Aborting Job. Could not get the Job ID from the Message with error:  %s",
					exception.getMessage());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR);
		}

		if (runningJobs.containsKey(jobId)) {
			// Cancel the Running Job
			runningJobs.get(jobId).cancel(true);
			// Remove it from the list of Running Jobs
			runningJobs.remove(jobId);
		}
	}

	/**
	 * Returns a list of the Job Ids that are currently being processed by this instance
	 * 
	 * @return The list of Job Ids
	 */
	public List<String> getRunningJobIds() {
		return new ArrayList<String>(runningJobs.keySet());
	}

}
