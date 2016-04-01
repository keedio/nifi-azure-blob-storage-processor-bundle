package com.keedio.nifi.processors.azure.blob;

import com.microsoft.azure.storage.blob.*;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.keedio.nifi.controllers.AzureBlobConnectionService;

import java.util.*;

/**
 * Created by luca on 30/03/16.
 */
public abstract class AbstractAzureCredentialsProviderProcessor extends AbstractProcessor {

    public enum OnExistsBehaviour {
        OVERWRITE, FAIL, APPEND
    }

    public static final Validator AZURE_STORAGE_CONNECTION_STRING_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();

            builder.subject(subject).input(input);

            try {
                FlowFile.KeyValidator.validateKey(input);
                builder.valid(true);
            } catch (final IllegalArgumentException e) {
                builder.valid(false).explanation(e.getMessage());
            }

            return builder.build();
        }
    };

    public static final PropertyDescriptor AZURE_STORAGE_CONTROLLER_SERVICE = new PropertyDescriptor.Builder()
            .name("Azure Storage Controller Service")
            .description("Azure Storage Controller Service. Used to shared the connection to an Azure Blob Storage container among different processors.")
            .identifiesControllerService(AzureBlobConnectionService.class)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("FlowFiles are routed to success relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("FlowFiles are routed to failure relationship").build();


    protected List<PropertyDescriptor> initPropertyDescriptors() {

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(AZURE_STORAGE_CONTROLLER_SERVICE);
        return descriptors;
    }

    protected Set<Relationship> initRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    protected void copyAttributes(String containerName, String blobObject, CloudBlobWrapper blob, Map<String, String> attributes) {
        BlobProperties p = blob.getProperties();
        addAttribute(attributes, "property.blobType", getProperty(p, "blobType"));
        addAttribute(attributes, "property.cacheControl", getProperty(p, "cacheControl"));
        addAttribute(attributes, "property.contentDisposition", getProperty(p, "contentDisposition"));
        addAttribute(attributes, "property.contentEncoding", getProperty(p, "contentEncoding"));
        addAttribute(attributes, "property.contentLanguage", getProperty(p, "contentLanguage"));
        addAttribute(attributes, "property.contentMD5", getProperty(p, "contentMD5"));
        addAttribute(attributes, "property.contentType",getProperty(p, "contentType"));

        CopyState cs = p.getCopyState();
        addAttribute(attributes, "property.copyState.id", getProperty(cs, "copyId"));
        addAttribute(attributes, "property.copyState.completionTime", getProperty(cs, "completionTime"));
        addAttribute(attributes, "property.copyState.status", getProperty(cs, "status", String.class));
        addAttribute(attributes, "property.copyState.source", getProperty(cs, "source"));
        addAttribute(attributes, "property.copyState.bytesCopied", getProperty(cs, "bytesCopied"));
        addAttribute(attributes, "property.copyState.totalBytes", getProperty(cs, "totalBytes"));
        addAttribute(attributes, "property.copyState.statusDescription", getProperty(cs, "statusDescription"));

        addAttribute(attributes, "property.etag", p.getEtag());

        String lastModified = DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(getProperty(p,"lastModified", Date.class));

        addAttribute(attributes, "property.lastModified", lastModified);
        addAttribute(attributes, "property.leaseStatus", getProperty(p, "leaseStatus"));
        addAttribute(attributes, "property.leaseState", getProperty(p, "leaseState"));
        addAttribute(attributes, "property.leaseDuration", getProperty(p, "leaseDuration"));
        addAttribute(attributes, "property.length", getProperty(p,"length"));
        addAttribute(attributes, "property.pageBlobSequenceNumber", getProperty(p, "pageBlobSequenceNumber"));
        addAttribute(attributes, "metadata.blobName", blobObject);
        addAttribute(attributes, "metadata.containerName", containerName);
    }

    private String getProperty(Object p, String propName){
        return getProperty(p, propName, String.class);
    }

    @SuppressWarnings("unchecked")
    private <T> T getProperty(Object p, String propName, Class<T> returnClazz) {
        T res = null;
        if (p != null) {
            try {
                Object tmp = (T)PropertyUtils.getProperty(p, propName);

                if (returnClazz.equals(String.class) &&
                        !tmp.getClass().equals(String.class)){

                    res = (T)tmp.toString();
                } else {
                    res = (T)tmp;
                }
            } catch (Exception e) {
                getLogger().debug(String.format("Could not get property %s", propName));
            }
        }
        return res;
    }

    protected void addAttribute(Map<String, String> attributes, String key, String value) {
        if (StringUtils.isNotEmpty(value)) {
            attributes.put(key, value);
        }
    }
}
