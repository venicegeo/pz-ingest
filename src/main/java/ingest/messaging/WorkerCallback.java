package ingest.messaging;

/**
 * Callback class to communicate the completion status of a Worker by the Thread
 * Manager.
 * 
 * @author Patrick.Doody
 * 
 */
public interface WorkerCallback {
	/**
	 * Fired when the Worker has finished running; success or fail.
	 * 
	 * @param jobId
	 *            The Job ID being processed that has completed
	 */
	void onComplete(String jobId);
}
