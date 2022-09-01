package com.cookpad.prism.objectstore;

import java.io.InputStream;

import com.cookpad.prism.dao.PrismStagingObject;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.services.s3.S3Client;

@RequiredArgsConstructor
public class StagingObjectStore {
    final private S3Client s3;

    public InputStream getStagingObject(PrismStagingObject stagingObject) {
        return this.s3.getObject(r -> r.bucket(stagingObject.getBucketName()).key(stagingObject.getObjectKey()));
    }
}
