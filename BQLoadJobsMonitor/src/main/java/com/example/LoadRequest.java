package com.example;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Objects;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"attributes", "payload"})
public class LoadRequest implements Serializable {

  @JsonProperty("attributes")
  public LoadRequestAttributes loadRequestAttributes;

  @JsonProperty("payload")
  public LoadRequestPayload loadRequestPayload;

  public LoadRequestAttributes getLoadRequestAttributes() {
    return loadRequestAttributes;
  }

  public void setLoadRequestAttributes(LoadRequestAttributes loadRequestAttributes) {
    this.loadRequestAttributes = loadRequestAttributes;
  }

  public LoadRequestPayload getLoadRequestPayload() {
    return loadRequestPayload;
  }

  public void setLoadRequestPayload(LoadRequestPayload loadRequestPayload) {
    this.loadRequestPayload = loadRequestPayload;
  }

  @Override
  public String toString() {
    return "LoadRequest{" + "attributes=" + loadRequestAttributes + ", payload=" + loadRequestPayload
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LoadRequest that = (LoadRequest) o;
    return Objects.equal(getLoadRequestAttributes(), that.getLoadRequestAttributes())
        && Objects.equal(getLoadRequestPayload(), that.getLoadRequestPayload());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getLoadRequestAttributes(), getLoadRequestPayload());
  }
}
