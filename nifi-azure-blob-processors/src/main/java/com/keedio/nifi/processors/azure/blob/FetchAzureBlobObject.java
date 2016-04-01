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
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.keedio.nifi.controllers.AzureBlobConnectionService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"Azure", "Blob", "Get", "Fetch"})
@CapabilityDescription("Retrieves the contents of an Azure Blob Object and writes it to the content of a FlowFile")
@SeeAlso({PutAzureBlobObject.class})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@WritesAttributes({
        @WritesAttribute(attribute="property.appendBlobCommittedBlockCount"),
        @WritesAttribute(attribute="property.blobType"),
        @WritesAttribute(attribute="property.cacheControl"),
        @WritesAttribute(attribute="property.contentDisposition"),
        @WritesAttribute(attribute="property.contentEncoding"),
        @WritesAttribute(attribute="property.contentLanguage"),
        @WritesAttribute(attribute="property.contentMD5"),
        @WritesAttribute(attribute="property.contentType"),
        @WritesAttribute(attribute="property.copyState.id"),
        @WritesAttribute(attribute="property.copyState.completionTime"),
        @WritesAttribute(attribute="property.copyState.status"),
        @WritesAttribute(attribute="property.copyState.source"),
        @WritesAttribute(attribute="property.copyState.bytesCopied"),
        @WritesAttribute(attribute="property.copyState.totalBytes"),
        @WritesAttribute(attribute="property.copyState.statusDescription"),
        @WritesAttribute(attribute="property.etag"),
        @WritesAttribute(attribute="property.lastModified"),
        @WritesAttribute(attribute="property.leaseStatus"),
        @WritesAttribute(attribute="property.leaseState"),
        @WritesAttribute(attribute="property.leaseDuration"),
        @WritesAttribute(attribute="property.length"),
        @WritesAttribute(attribute="property.pageBlobSequenceNumber"),
        @WritesAttribute(attribute="metadata.blobName"),
        @WritesAttribute(attribute="metadata.containerName")
})
public class FetchAzureBlobObject extends AbstractAzureCredentialsProviderProcessor {

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    public static final PropertyDescriptor AZURE_STORAGE_BLOB_OBJECT = new PropertyDescriptor.Builder()
            .name("Full path name of the blob object to retrieve")
            .description("Full path name of the blob object to retrieve")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = initPropertyDescriptors();
        descriptors.add(AZURE_STORAGE_BLOB_OBJECT);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = initRelationships();
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        final long startNanos = System.nanoTime();

        final String blobObject = context.getProperty(AZURE_STORAGE_BLOB_OBJECT).evaluateAttributeExpressions(flowFile).getValue();

        final AzureBlobConnectionService connectionService =
                context.getProperty(AZURE_STORAGE_CONTROLLER_SERVICE).asControllerService(AzureBlobConnectionService.class);

        CloudBlobContainer blobContainer;
        try {
            blobContainer = connectionService.getCloudBlobContainerReference();
        } catch (URISyntaxException | InvalidKeyException | StorageException e) {
            getLogger().error("Failed to retrieve Azure Object for {}; routing to failure", new Object[]{flowFile, e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        CloudBlobWrapper blockBlob = null;
        try {
            blockBlob = new CloudBlobWrapper("overwrite", blobObject, blobContainer);
            //blobContainer.getBlockBlobReference(blobObject);
        } catch (Exception e) {
            getLogger().error("Caught exception while retrieving blob object reference", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        InputStream blockBlobStream = null;
        ByteArrayOutputStream outputStream = null;
        try {
            outputStream = new ByteArrayOutputStream();
            blockBlob.download(outputStream);
            outputStream.flush();

            blockBlobStream = new ByteArrayInputStream(outputStream.toByteArray());

            flowFile = session.importFrom(blockBlobStream, flowFile);
        } catch (IOException | StorageException e) {
            getLogger().error("Caught exception while retrieving blob object", e);
        }  finally {
            IOUtils.closeQuietly(blockBlobStream);
            IOUtils.closeQuietly(outputStream);
        }

        final Map<String, String> attributes = new HashMap<>();

        copyAttributes(connectionService.getContainerName(), blobObject, blockBlob, attributes);

        if (!attributes.isEmpty()) {
            flowFile = session.putAllAttributes(flowFile, attributes);
        }

        session.transfer(flowFile, REL_SUCCESS);

        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully retrieved Blob Object for {} in {} millis; routing to success", new Object[]{flowFile, transferMillis});
        session.getProvenanceReporter().fetch(flowFile, blockBlob.toString(), transferMillis);
    }

}
