package com.cookpad.prism.objectstore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

@RequiredArgsConstructor
@Slf4j
public class PrismObjectStore implements SmallObjectStore, MergedObjectStore {
    final private S3Client s3;
    final private PrismTableLocator locator;

    @Override
    public InputStream getLiveObject(LocalDate dt, long objectId) {
        String key = locator.getLiveObjectKey(dt, objectId);
        log.debug("Get live object: {}", key);
        return s3.getObject(r -> r.bucket(locator.getBucketName()).key(key));
    }

    @Override
    public File getLiveObjectFile(LocalDate dt, long objectId) throws IOException {
        Path tmpPath = Files.createTempFile("prism-batch-live-", ".parquet").toAbsolutePath();
        try (InputStream in = this.getLiveObject(dt, objectId)) {
            Files.copy(in, tmpPath, StandardCopyOption.REPLACE_EXISTING);
        }
        return tmpPath.toFile();
    }

    @Override
    public String putLiveObjectFile(LocalDate dt, long objectId, File content) {
        String key = locator.getLiveObjectKey(dt, objectId);
        log.info("PutObject (live) key={}", key);
        s3.putObject(r -> r.bucket(locator.getBucketName()).key(key), RequestBody.fromFile(content));
        return key;
    }

    @Override
    public InputStream getDelayedObject(LocalDate dt, long objectId) {
        String key = locator.getDelayedObjectKey(dt, objectId);
        log.debug("Get delayed object: {}", key);
        return s3.getObject(r -> r.bucket(locator.getBucketName()).key(key));
    }

    @Override
    public File getDelayedObjectFile(LocalDate dt, long objectId) throws IOException {
        Path tmpPath = Files.createTempFile("prism-batch-delayed-", ".parquet").toAbsolutePath();
        try (InputStream in = this.getDelayedObject(dt, objectId)) {
            Files.copy(in, tmpPath, StandardCopyOption.REPLACE_EXISTING);
        }
        return tmpPath.toFile();
    }

    @Override
    public String putDelayedObjectFile(LocalDate dt, long objectId, File content) {
        String key = locator.getDelayedObjectKey(dt, objectId);
        log.info("PutObject (delayed) key={}", key);
        s3.putObject(r -> r.bucket(locator.getBucketName()).key(key), RequestBody.fromFile(content));
        return key;
    }

    @Override
    public InputStream getMergedObject(LocalDate dt, long lowerBound, long upperBound) {
        String key = locator.getMergedObjectKey(dt, lowerBound, upperBound);
        log.debug("Get merged object: {}", key);
        return s3.getObject(r -> r.bucket(locator.getBucketName()).key(key));
    }

    @Override
    public File getMergedObjectFile(LocalDate dt, long lowerBound, long upperBound) throws IOException {
        Path tmpPath = Files.createTempFile("prism-batch-merged-", ".parquet").toAbsolutePath();
        try (InputStream in = this.getMergedObject(dt, lowerBound, upperBound)) {
            Files.copy(in, tmpPath, StandardCopyOption.REPLACE_EXISTING);
        }
        return tmpPath.toFile();
    }

    @Override
    public String putMergedObjectFile(LocalDate dt, long lowerBound, long upperBound, File content) {
        String key = locator.getMergedObjectKey(dt, lowerBound, upperBound);
        log.info("PutObject (merged) key={}", key);
        s3.putObject(r -> r.bucket(locator.getBucketName()).key(key), RequestBody.fromFile(content));
        return key;
    }

    @Override
    public String putMergedPartitionManifest(LocalDate dt, long manifestVersion, String content) {
        String key = locator.getMergedPartitionManifestKey(dt, manifestVersion);
        log.info("Put partition manifest: {}", key);
        Map<String, String> metadata = new HashMap<>();
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        metadata.put("content-length", String.valueOf(contentBytes.length));
        log.info("PutObject (manifest) key={}", key);
        s3.putObject(r -> r.bucket(locator.getBucketName()).key(key).metadata(metadata), RequestBody.fromBytes(contentBytes));
        return key;
    }
}
