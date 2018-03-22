package com.tmwsystems.streaming.tests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.sam.enrichment.processor.RestApiSinkProcessor;


public class RestSinkProcessorTest {
	
	protected static final Logger LOG = LoggerFactory.getLogger(RestSinkProcessorTest.class);
	private static final Object TEST_CONFIG_REST_URL = "http://10.60.13.78:8080/recordFreightVisibility";
	
	
	@Test
	public void testHREnrichmentNonSecureCluster() throws Exception {
		RestApiSinkProcessor enrichProcessor = new RestApiSinkProcessor();
		Map<String, Object> processorConfig = createHREnrichmentConfig();
		enrichProcessor.validateConfig(processorConfig);
		enrichProcessor.initialize(processorConfig);
		
		List<StreamlineEvent> eventResults = enrichProcessor.process(createStreamLineEvent());

		LOG.info("Result of enrichment is: " + ReflectionToStringBuilder.toString(eventResults));
		

	}
	
	
	private StreamlineEvent createStreamLineEvent() {
		Map<String, Object> keyValues = new HashMap<String, Object>();
		keyValues.put("payload", "{\"id\":\"137-615-0057335\",\"data\":\"{\\\"location\\\":{\\\"lat\\\":\\\"130595\\\",\\\"lon\\\":\\\"350384\\\",\\\"gpstimestamp\\\":\\\"2018-01-02 14:59:11.527\\\"}, \\\"address\\\":{\\\"street\\\":\\\"abcd\\\",\\\"city\\\":\\\"city-1\\\",\\\"state\\\":\\\"OH\\\",\\\"zip\\\":\\\"abc-123\\\"}\",\"driverId\":\"137\",\"bol\":615,\"truckId\":\"615\"}}");
		
		StreamlineEvent event = StreamlineEventImpl.builder().build().addFieldsAndValues(keyValues);
		
		System.out.println("Input StreamLIne event is: " + ReflectionToStringBuilder.toString(event));

		
		return event;
	}

	private Map<String, Object> createHREnrichmentConfig() {
		Map<String, Object> processorConfig = new HashMap<String, Object>();
		processorConfig.put(RestApiSinkProcessor.CONFIG_REST_URL,TEST_CONFIG_REST_URL);
		return processorConfig;
	}

}
