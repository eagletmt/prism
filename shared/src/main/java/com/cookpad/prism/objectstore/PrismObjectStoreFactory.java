package com.cookpad.prism.objectstore;

import com.cookpad.prism.dao.PrismTable;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.services.s3.S3Client;

@RequiredArgsConstructor
public class PrismObjectStoreFactory {
    final private S3Client s3;
    final private PrismTableLocatorFactory objectLocator;

    public PrismObjectStore create(PrismTable table) {
        PrismTableLocator locator = this.objectLocator.build(table);
        return new PrismObjectStore(this.s3, locator);
    }
}
