package com.example;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Objects;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"bundleTimestamp"})
public class Attributes implements Serializable {
  @JsonProperty("bundleTimestamp")
  private String bundleTimestamp;

  public String getBundleTimestamp() {
    return bundleTimestamp;
  }

  public void setBundleTimestamp(String bundleTimestamp) {
    this.bundleTimestamp = bundleTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Attributes that = (Attributes) o;
    return Objects.equal(getBundleTimestamp(), that.getBundleTimestamp());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getBundleTimestamp());
  }

  @Override
  public String toString() {
    return "Attributes{" + "bundleTimestamp='" + bundleTimestamp + '\'' + '}';
  }
}
