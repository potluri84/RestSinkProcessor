package com.sam.enrichment.processor;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;


public class RestApiSinkProcessor implements CustomProcessorRuntime {
	protected static final Logger LOG = LoggerFactory.getLogger(RestApiSinkProcessor.class);
	public static final String CONFIG_REST_URL = "restURL";

	Map<String, Object> config;
	private String restURL;

	public RestApiSinkProcessor()
	{
		
	}
	
	
	public void initialize(Map<String, Object> config) {
		LOG.info("Initializing + " + RestApiSinkProcessor.class.getName());

		this.restURL = ((String) config.get(CONFIG_REST_URL)).trim();
		LOG.info("The configured URL is: " + restURL);
	}

	public void validateConfig(Map<String, Object> config) throws ConfigException {
	}

	public List<StreamlineEvent> process(StreamlineEvent streamlineEvent) throws ProcessingException {
		LOG.info("Event[" + streamlineEvent + "] about to be enriched");
		StreamlineEvent enrichedEvent = null;
		Map<String, Object> enrichValues = null;
		try
		{
		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
		builder.putAll(streamlineEvent);

		enrichValues = restCall(streamlineEvent);
		LOG.info("Enriching events[" + streamlineEvent + "]  with the following enriched values: " + enrichValues);
		builder.putAll(enrichValues);

		enrichedEvent = builder.dataSourceId(streamlineEvent.getDataSourceId()).build();
		LOG.info("Enriched StreamLine Event is: " + enrichedEvent);

		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			throw new ProcessingException(ex.getMessage(),ex.getCause());
		}
		List<StreamlineEvent> newEvents = Collections.<StreamlineEvent>singletonList(enrichedEvent);

		return newEvents;

	}

	private Map<String, Object> restCall(StreamlineEvent streamlineEvent) throws Exception {
		Map<String, Object> enrichedValues = new HashMap<String, Object>();

		try
		{
	      
		LOG.info(this.restURL);
 
		 URL url = new URL(this.restURL);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/json");

			String input = (String) streamlineEvent.get("payload");

			OutputStream os = conn.getOutputStream();
			os.write(input.getBytes());
			os.flush();

			if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
				throw new RuntimeException("Failed : HTTP error code : "
					+ conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					(conn.getInputStream())));

			
			conn.disconnect();
		 
		
		enrichedValues.put("requestUrl", this.restURL);
		enrichedValues.put("HTTPOutput",conn.getResponseCode());

		}
		catch(Exception ex)
		{
			LOG.error(ex.getMessage());
			throw ex;
		}
		return enrichedValues;
	}

	public void cleanup() {
		LOG.debug("Cleaning up");
	}

}