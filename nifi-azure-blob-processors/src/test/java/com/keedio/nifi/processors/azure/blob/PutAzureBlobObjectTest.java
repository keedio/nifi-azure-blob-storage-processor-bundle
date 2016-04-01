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

import com.microsoft.azure.storage.StorageException;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.keedio.nifi.controllers.AzureBlobConnectionService;
import org.keedio.nifi.controllers.AzureBlobConnectionServiceImpl;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.keedio.nifi.processors.azure.blob.AbstractAzureCredentialsProviderProcessor.*;
import static com.keedio.nifi.processors.azure.blob.FetchAzureBlobObject.AZURE_STORAGE_BLOB_OBJECT;
import static com.keedio.nifi.processors.azure.blob.PutAzureBlobObject.*;
import static org.junit.Assert.*;
import static org.keedio.nifi.controllers.AzureBlobConnectionService.AZURE_STORAGE_CONNECTION_STRING;
import static org.keedio.nifi.controllers.AzureBlobConnectionService.AZURE_STORAGE_CONTAINER_NAME;
import static com.keedio.nifi.processors.azure.blob.AbstractAzureCredentialsProviderProcessor.OnExistsBehaviour.*;

public class PutAzureBlobObjectTest {

    public static final String TEST_PUT_AZURE_BLOB_OBJECT_BLOCKBLOB_TMP = "test/put/azure/blob/object/blockblob.tmp";
    public static final String TEST_PUT_AZURE_BLOB_OBJECT_BLOCKBLOB_APPEND_TMP = "test/put/azure/blob/object/blockblob-append.tmp";
    private TestRunner testRunner;
    private PutAzureBlobObject processor;

    @Before
    public void init() {
        processor = new PutAzureBlobObject();
        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setValidateExpressionUsage(false);
    }

    @Test
    public void testPutAzureBlobObjectProcessorAlreadyExists() throws IOException, ParseException, InitializationException {
        String connectionString = System.getProperty("storageConnectionString");
        String containerName = System.getProperty("containerName");

        Assume.assumeTrue(StringUtils.isNoneEmpty(connectionString, containerName));

        AzureBlobConnectionService connectionService = new AzureBlobConnectionServiceImpl();
        Map<String, String> controllerProperties = new HashMap<>();
        controllerProperties.put(AZURE_STORAGE_CONNECTION_STRING.getName(), connectionString);
        controllerProperties.put(AZURE_STORAGE_CONTAINER_NAME.getName(), containerName);

        testRunner.addControllerService("my-put-test-connection-service", connectionService, controllerProperties);

        testRunner.enableControllerService(connectionService);
        testRunner.assertValid(connectionService);
        testRunner.setProperty(AZURE_STORAGE_BLOB_OBJECT, "blockblob.tmp");
        testRunner.setProperty(AZURE_STORAGE_CONTROLLER_SERVICE, connectionService.getIdentifier());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.FILENAME.key(), "blockblob.tmp");

        testRunner.enqueue(getResourcePath("/blockblob.tmp"), attrs);
        testRunner.assertValid();

        testRunner.run(1);
        testRunner.assertTransferCount(REL_FAILURE,1);
        testRunner.assertTransferCount(REL_SUCCESS,0);
    }

    @Test
    public void testPutAzureBlobObjectProcessorOverride() throws IOException, ParseException, InitializationException, InterruptedException, InvalidKeyException, StorageException, URISyntaxException {
        CloudBlobWrapper blob = null;
        try {

            String connectionString = System.getProperty("storageConnectionString");
            String containerName = System.getProperty("containerName");

            Assume.assumeTrue(StringUtils.isNoneEmpty(connectionString, containerName));

            AzureBlobConnectionService connectionService = new AzureBlobConnectionServiceImpl();
            Map<String, String> controllerProperties = new HashMap<>();
            controllerProperties.put(AZURE_STORAGE_CONNECTION_STRING.getName(), connectionString);
            controllerProperties.put(AZURE_STORAGE_CONTAINER_NAME.getName(), containerName);

            testRunner.addControllerService("my-put-test-connection-service", connectionService, controllerProperties);

            testRunner.enableControllerService(connectionService);
            testRunner.assertValid(connectionService);
            testRunner.setProperty(AZURE_STORAGE_CONTROLLER_SERVICE, connectionService.getIdentifier());

            Date now = new Date();

            Thread.sleep(1100);

            testRunner.setProperty(AZURE_STORAGE_BEHAVIOUR_IF_BLOB_EXISTS, "overwrite");

            final String DYNAMIC_ATTRIB_KEY_1 = "runTimestamp";
            final String DYNAMIC_ATTRIB_VALUE_1 = String.valueOf(System.nanoTime());
            final String DYNAMIC_ATTRIB_KEY_2 = "testUploader";
            final String DYNAMIC_ATTRIB_VALUE_2 = "KEEDIO";

            PropertyDescriptor testAttrib1 = processor.getSupportedDynamicPropertyDescriptor(DYNAMIC_ATTRIB_KEY_1);
            testRunner.setProperty(testAttrib1, DYNAMIC_ATTRIB_VALUE_1);
            PropertyDescriptor testAttrib2 = processor.getSupportedDynamicPropertyDescriptor(DYNAMIC_ATTRIB_KEY_2);
            testRunner.setProperty(testAttrib2, DYNAMIC_ATTRIB_VALUE_2);

            final Map<String, String> attrs = new HashMap<>();
            attrs.put(CoreAttributes.FILENAME.key(), TEST_PUT_AZURE_BLOB_OBJECT_BLOCKBLOB_TMP);

            testRunner.enqueue(getResourcePath("/blockblob.tmp"), attrs);
            testRunner.assertValid();

            testRunner.run(1);
            testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);

            MockFlowFile mockFlowFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
            Map<String, String> attributes = mockFlowFile.getAttributes();
            assertTrue(attributes.size() >= 3);

            assertEquals(TEST_PUT_AZURE_BLOB_OBJECT_BLOCKBLOB_TMP, attributes.get("metadata.blobName"));
            assertEquals("blobbasics3d8a33d05e7940dabbb92476fc4fb345", attributes.get("metadata.containerName"));
            assertNotNull(attributes.get("property.lastModified"));

            Date lastModified =
                    DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.parse(attributes.get("property.lastModified"));

            assertTrue(lastModified.after(now) || lastModified.equals(now));

            assertTrue(Long.parseLong(attributes.get("property.length")) >= 0);
            assertFalse(attributes.containsKey("property.contentDisposition"));
            assertFalse(attributes.containsKey("property.copyState"));

            File localTestFile = new File("src/test/resources/blockblob.tmp");
            mockFlowFile.assertContentEquals(localTestFile);

            blob = new CloudBlobWrapper(
                    TEST_PUT_AZURE_BLOB_OBJECT_BLOCKBLOB_TMP,
                    connectionService.getCloudBlobContainerReference());

            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                blob.download(os);
                assertEquals(localTestFile.length(), os.toByteArray().length);
            }
        } finally {
            if (blob != null)
                blob.deleteIfExists();
        }
    }

    @Test
    public void testPutAzureBlobObjectProcessorAppend() throws IOException, ParseException, InitializationException, InterruptedException, InvalidKeyException, StorageException, URISyntaxException {
        CloudBlobWrapper blob = null;
        try {
            String connectionString = System.getProperty("storageConnectionString");

            String containerName = System.getProperty("containerName");

            Assume.assumeTrue(StringUtils.isNoneEmpty(connectionString, containerName));

            AzureBlobConnectionService connectionService = new AzureBlobConnectionServiceImpl();
            Map<String, String> controllerProperties = new HashMap<>();
            controllerProperties.put(AZURE_STORAGE_CONNECTION_STRING.getName(), connectionString);
            controllerProperties.put(AZURE_STORAGE_CONTAINER_NAME.getName(), containerName);

            testRunner.addControllerService("my-put-test-connection-service", connectionService, controllerProperties);

            testRunner.enableControllerService(connectionService);
            testRunner.assertValid(connectionService);
            testRunner.setProperty(AZURE_STORAGE_CONTROLLER_SERVICE, connectionService.getIdentifier());

            testRunner.setProperty(AZURE_STORAGE_BEHAVIOUR_IF_BLOB_EXISTS, "append");

            final String DYNAMIC_ATTRIB_KEY_1 = "runTimestamp";
            final String DYNAMIC_ATTRIB_VALUE_1 = String.valueOf(System.nanoTime());
            final String DYNAMIC_ATTRIB_KEY_2 = "testUploader";
            final String DYNAMIC_ATTRIB_VALUE_2 = "KEEDIO";

            PropertyDescriptor testAttrib1 = processor.getSupportedDynamicPropertyDescriptor(DYNAMIC_ATTRIB_KEY_1);
            testRunner.setProperty(testAttrib1, DYNAMIC_ATTRIB_VALUE_1);
            PropertyDescriptor testAttrib2 = processor.getSupportedDynamicPropertyDescriptor(DYNAMIC_ATTRIB_KEY_2);
            testRunner.setProperty(testAttrib2, DYNAMIC_ATTRIB_VALUE_2);

            final Map<String, String> attrs = new HashMap<>();
            attrs.put(CoreAttributes.FILENAME.key(), TEST_PUT_AZURE_BLOB_OBJECT_BLOCKBLOB_APPEND_TMP);

            testRunner.enqueue(getResourcePath("/blockblob.tmp"), attrs);
            testRunner.assertValid();

            testRunner.run(1);
            testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);

            File localTestFile = new File("src/test/resources/blockblob.tmp");

            blob = new CloudBlobWrapper(
                    TEST_PUT_AZURE_BLOB_OBJECT_BLOCKBLOB_APPEND_TMP,
                    connectionService.getCloudBlobContainerReference());

            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                blob.download(os);
                assertEquals(localTestFile.length(), os.toByteArray().length);
            }

            testRunner.enqueue(getResourcePath("/blockblob.tmp"), attrs);
            testRunner.assertValid();

            testRunner.run(1);
            testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

            blob = new CloudBlobWrapper(
                    TEST_PUT_AZURE_BLOB_OBJECT_BLOCKBLOB_APPEND_TMP,
                    connectionService.getCloudBlobContainerReference());

            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                blob.download(os);
                assertEquals(2*localTestFile.length(), os.toByteArray().length);
            }
        } finally {
            if (blob != null)
                blob.deleteIfExists();
        }
    }

    protected Path getResourcePath(String resourceName) {
        Path path = null;

        try {
            path = Paths.get(getClass().getResource(resourceName).toURI());
        } catch (URISyntaxException e) {
            Assert.fail("Resource: " + resourceName + " does not exist" + e.getLocalizedMessage());
        }

        return path;
    }

}
