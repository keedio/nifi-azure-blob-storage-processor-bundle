package com.keedio.nifi.processors.azure.blob;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.util.HashMap;

import static com.keedio.nifi.processors.azure.blob.AbstractAzureCredentialsProviderProcessor.*;

/**
 * Created by luca on 31/03/16.
 */
public class CloudBlobWrapper {
    private CloudBlob wrapped;
    private OnExistsBehaviour behaviour;
    private String blobName;

    public CloudBlobWrapper(String blobName, CloudBlobContainer container)
            throws URISyntaxException, StorageException {
        this("fail", blobName, container);
    }
    public CloudBlobWrapper(String behaviour, String blobName, CloudBlobContainer container)
            throws URISyntaxException, StorageException {

        this.behaviour = OnExistsBehaviour.valueOf(behaviour.toUpperCase());
        this.blobName = blobName;

        switch (this.behaviour) {
            case OVERWRITE:
                wrapped = container.getBlockBlobReference(blobName);
                break;
            case APPEND:
            case FAIL:
                wrapped = container.getAppendBlobReference(blobName);
                break;
            default:
        }
    }

    public boolean exists() throws StorageException {
        return wrapped.exists();
    }

    public void createOrReplace() throws StorageException {

        boolean exists = exists();

        switch (behaviour) {
            case FAIL:
                throw new StorageException(
                        "HTTP_BAD_REQUEST",
                        "blob with name " + blobName + " already exists",
                        HttpURLConnection.HTTP_BAD_REQUEST, null, null);

            case OVERWRITE:
                CloudBlockBlob instance = (CloudBlockBlob) wrapped;
                if (exists) {
                    instance.deleteIfExists();
                }

                break;
            case APPEND:
                CloudAppendBlob appendBlob = (CloudAppendBlob) wrapped;
                if (!exists) {
                    appendBlob.createOrReplace();
                }
        }
    }

    public boolean deleteIfExists() throws StorageException {
        return wrapped.deleteIfExists();
    }

    public void download(OutputStream os) throws StorageException {
        wrapped.download(os);
    }

    public void upload(InputStream storageStream, int length) throws StorageException, IOException {
        switch (behaviour) {
            case FAIL:
                throw new StorageException(
                        "HTTP_BAD_REQUEST",
                        "blob with name " + blobName + " already exists",
                        HttpURLConnection.HTTP_BAD_REQUEST, null, null);
            case APPEND:
                CloudAppendBlob appendBlob = (CloudAppendBlob) wrapped;
                appendBlob.append(storageStream, length);
                break;
            case OVERWRITE:
                CloudBlockBlob blockBlob = (CloudBlockBlob) wrapped;
                blockBlob.upload(storageStream, length);
        }
    }

    public void setMetadata(HashMap<String, String> metadata) {
        wrapped.setMetadata(metadata);
    }

    public BlobProperties getProperties() {
        return wrapped.getProperties();
    }
}
