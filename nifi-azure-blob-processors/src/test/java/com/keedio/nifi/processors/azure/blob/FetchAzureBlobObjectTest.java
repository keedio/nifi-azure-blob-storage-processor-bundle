/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.keedio.nifi.processors.azure.blob;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.keedio.nifi.controllers.AzureBlobConnectionService;
import org.keedio.nifi.controllers.AzureBlobConnectionServiceImpl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.keedio.nifi.processors.azure.blob.AbstractAzureCredentialsProviderProcessor.*;
import static com.keedio.nifi.processors.azure.blob.FetchAzureBlobObject.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.keedio.nifi.controllers.AzureBlobConnectionService.*;

public class FetchAzureBlobObjectTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(FetchAzureBlobObject.class);
        testRunner.setValidateExpressionUsage(false);


    }

    @Test
    public void testFetchAzureBlobObjectProcessor() throws IOException, InitializationException {

        String connectionString = System.getProperty("storageConnectionString");
        String containerName = System.getProperty("containerName");
        Assume.assumeTrue(StringUtils.isNoneEmpty(connectionString, containerName));

        AzureBlobConnectionService connectionService = new AzureBlobConnectionServiceImpl();

        Map<String, String> controllerProperties = new HashMap<>();
        controllerProperties.put(AZURE_STORAGE_CONNECTION_STRING.getName(), connectionString);
        controllerProperties.put(AZURE_STORAGE_CONTAINER_NAME.getName(), containerName);

        testRunner.addControllerService("my-fetch-test-connection-service", connectionService, controllerProperties);

        testRunner.enableControllerService(connectionService);
        testRunner.assertValid(connectionService);
        testRunner.setProperty(AZURE_STORAGE_BLOB_OBJECT, "blockblob.tmp");
        testRunner.setProperty(AZURE_STORAGE_CONTROLLER_SERVICE, connectionService.getIdentifier());

        final Map<String, String> attrs = new HashMap<>();
        testRunner.enqueue(new byte[0], attrs);
        testRunner.run(1);
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile mockFlowFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        Map<String,String> attributes = mockFlowFile.getAttributes();
        assertEquals(15, attributes.size());

        assertEquals("36IiBrsABugPVI6mMnvJTA==", attributes.get("property.contentMD5"));
        assertEquals("application/octet-stream", attributes.get("property.contentType"));
        assertEquals("blockblob.tmp", attributes.get("metadata.blobName"));
        assertEquals("blobbasics3d8a33d05e7940dabbb92476fc4fb345", attributes.get("metadata.containerName"));

        assertEquals("2016-03-30T17:31:31+02:00", attributes.get("property.lastModified"));
        assertEquals("247682", attributes.get("property.length"));
        assertFalse(attributes.containsKey("property.contentDisposition"));
        assertFalse(attributes.containsKey("property.copyState"));

        mockFlowFile.assertContentEquals(new File("src/test/resources/blockblob.tmp"));
    }

}
