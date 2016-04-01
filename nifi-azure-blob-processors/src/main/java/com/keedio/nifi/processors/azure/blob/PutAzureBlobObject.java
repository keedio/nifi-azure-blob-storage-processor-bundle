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
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.keedio.nifi.controllers.AzureBlobConnectionService;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"Azure", "Blob", "Put", "Archive"})
@CapabilityDescription("Put FlowFiles to an Azure Blob object")
@SeeAlso({FetchAzureBlobObject.class})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "The name of a User-Defined Metadata field to add to the Azure Blob Object",
        value = "The value of a User-Defined Metadata field to add to the Blob Object",
        description = "Allows user-defined metadata to be added to the Blob object as key/value pairs",
        supportsExpressionLanguage = true)
@ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the Azure Blob object")
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
@TriggerSerially
@SupportsBatching
public class PutAzureBlobObject extends AbstractAzureCredentialsProviderProcessor {

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }


    /**
     * AWS credentials provider service
     *
     * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    public static final PropertyDescriptor AZURE_STORAGE_BEHAVIOUR_IF_BLOB_EXISTS = new PropertyDescriptor.Builder()
            .name("fail if blob exists")
            .description("What to do when output blob already exists.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .allowableValues("fail", "overwrite", "append")
            .defaultValue("fail")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = initPropertyDescriptors();
        descriptors.add(AZURE_STORAGE_BEHAVIOUR_IF_BLOB_EXISTS);

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


        final String behaviour =
                context.getProperty(AZURE_STORAGE_BEHAVIOUR_IF_BLOB_EXISTS)
                        .evaluateAttributeExpressions(flowFile).getValue();

        final FlowFile ff = flowFile;

        final Map<String, String> attributes = new HashMap<>();
        final String ffFilename = ff.getAttributes().get(CoreAttributes.FILENAME.key());

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

        try {

            final CloudBlobWrapper blob = new CloudBlobWrapper(behaviour, ffFilename, blobContainer);

            final HashMap<String, String> userMetadata = new HashMap<>();
            for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                if (entry.getKey().isDynamic()) {
                    final String value = context.getProperty(
                            entry.getKey()).evaluateAttributeExpressions(ff).getValue();
                    userMetadata.put(entry.getKey().getName(), value);
                }
            }
            userMetadata.put("blobName", ffFilename);
            userMetadata.put("containerName", connectionService.getContainerName());

            blob.setMetadata(userMetadata);

            try {
                blob.createOrReplace();
            } catch (StorageException e) {
                getLogger().error(e.getMessage(),e);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream inputStream) throws IOException {

                    try {
                        blob.upload(inputStream, -1);
                    } catch (StorageException e) {
                        getLogger().error("Caught exception while appending data to blob object reference", e);
                        throw new RuntimeException(e);
                    }
                }
            });

            copyAttributes(connectionService.getContainerName(), ffFilename, blob, attributes);

            if (!attributes.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }

            session.transfer(flowFile, REL_SUCCESS);

            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            getLogger().info("Successfully put Blob Object {} in {} millis; routing to success", new Object[]{flowFile, transferMillis});
            session.getProvenanceReporter().fetch(flowFile, blob.toString(), transferMillis);

        } catch (StorageException e) {
            getLogger().error("Caught exception while retrieving blob object reference", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        } catch (URISyntaxException e) {
            getLogger().error("Invalid blob URI: " + ffFilename, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }

    }

}
