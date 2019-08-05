package com.example;

import com.google.gson.Gson;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.avro.reflect.Nullable;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashSet;

public class Bundle implements Serializable {

  /*
   * TODO: Make Bundle AvroCoder Compatible
   */
  private static final Gson gson = new Gson();
  private Long bundleSize;
  private LinkedHashSet<String> bundledFiles;
  @Nullable
  private String bundlePrefix;

  Bundle() {
    this.bundleSize = 0L;
    this.bundledFiles = new LinkedHashSet<>();
  }

  void add(String fileName, Long size) {
    boolean isNewFileAdded = this.bundledFiles.add(fileName);
    if (isNewFileAdded) {
      this.bundleSize += size;
    }
  }

  private boolean isBundlePrefixAvailable() {
    return !this.bundlePrefix.equals("");
  }

  public Long getBundleSize() {
    return this.bundleSize;
  }

  public LinkedHashSet<String> getBundledFiles() {
    return this.bundledFiles;
  }

  public String getBundlePrefix() {
    return this.bundlePrefix;
  }

  public void setBundlePrefix() {
    this.bundlePrefix = StringUtils.getCommonPrefix(this.bundledFiles.toArray(new String[0]));
  }

  @Override
  public String toString() {
    HashMap<String, Object> bundleMap = new HashMap<>();
    bundleMap.put("bundledFiles", this.getBundledFiles());
    bundleMap.put("bundleSizeBytes", this.getBundleSize());
    bundleMap.put("filesCount", this.getBundledFiles().size());
    if (isBundlePrefixAvailable()) {
      bundleMap.put("bundlePrefix", this.getBundlePrefix());
    }
    return gson.toJson(bundleMap);
  }
}
