/**
 * Mule Anypoint Template
 * Copyright (c) MuleSoft, Inc.
 * All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.api.MuleEvent;
import org.mule.context.notification.NotificationException;
import org.mule.modules.salesforce.bulk.EnrichedSaveResult;
import org.mule.tck.probe.PollingProber;
import org.mule.tck.probe.Prober;
import org.mule.templates.test.utils.ListenerProbe;
import org.mule.templates.test.utils.PipelineSynchronizeListener;

import com.mulesoft.module.batch.BatchTestHelper;

/**
 * The objective of this class is to validate the correct behavior of the flows
 * for this Mule Template that make calls to external systems.
 */
public class BusinessLogicIT extends AbstractTemplateTestCase {
	private static final String KEY_LAST_MODIFIED_DATE = "LastModifiedDate";
	private static final String KEY_ID = "Id";
	private static final String KEY_NAME = "Name";
	private static final String KEY_CODE = "ProductCode";
	
	private static final String POLL_FLOW_NAME = "triggerFlow";
	private static final String PRODUCT_NAME = "Product Test Name";
	private static final String PRODUCT_CODE = "TestSF2SAP";
	
	private static final Logger LOGGER = LogManager.getLogger(BusinessLogicIT.class);
	
	private static final int TIMEOUT_SEC = 120;

	private final Prober pollProber = new PollingProber(60000, 1000);
	private final PipelineSynchronizeListener pipelineListener = new PipelineSynchronizeListener(POLL_FLOW_NAME);

	private BatchTestHelper helper;
	private Map<String, Object> product;
	
	@BeforeClass
	public static void init() {
		System.setProperty("watermark.default.expression",
				"#[groovy: new Date(System.currentTimeMillis() - 10000).format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", TimeZone.getTimeZone('UTC'))]");
	}

	@Before
	public void setUp() throws Exception {
		stopFlowSchedulers(POLL_FLOW_NAME);
		registerListeners();
		helper = new BatchTestHelper(muleContext);

		// prepare test data
		product = createSalesforceProduct();
	}

	@After
	public void tearDown() throws Exception {
		stopFlowSchedulers(POLL_FLOW_NAME);
		// delete previously created product from DB by matching ID
		final Map<String, Object> prod = new HashMap<String, Object>();
		prod.put(KEY_NAME, product.get(KEY_NAME));
		prod.put(KEY_ID, product.get(KEY_ID));

		deleteMaterialFromSap(prod);
		deleteProductFromSalesforce(prod);
	}

	@Test
	public void testMainFlow() throws Exception {
		// Run poll and wait for it to run
		runSchedulersOnce(POLL_FLOW_NAME);
		waitForPollToRun();

		// Wait for the batch job executed by the poll flow to finish
		helper.awaitJobTermination(TIMEOUT_SEC * 1000, 500);
		helper.assertJobWasSuccessful();

		final MuleEvent event = runFlow("queryProductFromSapFlow", product);
		final List<?> payload = (List<?>) event.getMessage().getPayload();

		// print result
		for (Object acc : payload){
			LOGGER.info("material from SAP response: " + acc);
		}

		Assert.assertEquals("The product should have been sync", 1, payload.size());
		Assert.assertEquals("The product name should match", product.get(KEY_NAME), ((Map<?, ?>) payload.get(0)).get(KEY_NAME));
	}

	private void deleteProductFromSalesforce(final Map<String, Object> acc) throws Exception {
		List<Object> idList = new ArrayList<Object>();
		idList.add(acc.get(KEY_ID));
		runFlow("deleteProductsFromSalesforceFlow", idList);
	}

	private void deleteMaterialFromSap(final Map<String, Object> product) throws Exception {
		final MuleEvent event = runFlow("deleteProductsFromSapFlow", product);
		final Object result = event.getMessage().getPayload();
		LOGGER.info("deleteMaterialFromSap result: " + result);
	}

	private Map<String, Object> createSalesforceProduct() throws Exception {
		final Map<String, Object> product = new HashMap<String, Object>();
		product.put(KEY_NAME, PRODUCT_NAME + System.currentTimeMillis());
		product.put(KEY_CODE, PRODUCT_CODE + System.currentTimeMillis());
		
		final MuleEvent event = runFlow("createProductsInSalesforceFlow", Collections.singletonList(product));
		final List<?> result = (List<?>) event.getMessage().getPayload();
		
		// store Id into our product
		for (Object item : result) {
			LOGGER.info("response from createProductsInSalesforceFlow: " + item);
			product.put(KEY_ID, ((EnrichedSaveResult) item).getId());
			product.put(KEY_LAST_MODIFIED_DATE, ((EnrichedSaveResult) item).getPayload().getField(KEY_LAST_MODIFIED_DATE));
		}
		return product;
	}

	private void registerListeners() throws NotificationException {
		muleContext.registerListener(pipelineListener);
	}

	private void waitForPollToRun() {
		LOGGER.info("Waiting for poll to run once...");
		pollProber.check(new ListenerProbe(pipelineListener));
		LOGGER.info("Poll flow done");
	}

}
