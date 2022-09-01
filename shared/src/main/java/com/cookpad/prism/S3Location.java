package com.cookpad.prism;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class S3Location {
    final private String bucket;
    final private String key;
}
