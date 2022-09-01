package com.cookpad.prism.stream.filequeue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import com.cookpad.prism.S3Location;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.services.s3.S3Client;

@RequiredArgsConstructor
public class S3QueueDownloader {
    private final S3Client s3;

    public FileQueue download(S3Location queueObjectUrl) throws IOException {
        String key = queueObjectUrl.getKey();
        Path tmpPath = Files.createTempFile("prism-rebuild-queue-", ".queue").toAbsolutePath();
        try (InputStream in = s3.getObject(r -> r.bucket(queueObjectUrl.getBucket()).key(key))) {
            Files.copy(in, tmpPath, StandardCopyOption.REPLACE_EXISTING);
        }
        if (key.endsWith(".gz")) {
            return FileQueue.fromGzipFile(tmpPath.toFile());
        } else {
            return FileQueue.fromPlainFile(tmpPath.toFile());
        }
    }
}
