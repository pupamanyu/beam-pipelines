package com.example;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Objects;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"attributes", "payload"})
public class LoaderEnvelope implements Serializable {

  @JsonProperty("attributes")
  public LoaderEnvelopeAttributes loaderEnvelopeAttributes;

  @JsonProperty("payload")
  public LoadRequest loadRequest;

  public LoaderEnvelopeAttributes getLoaderEnvelopeAttributes() {
    return loaderEnvelopeAttributes;
  }

  public void setLoaderEnvelopeAttributes(LoaderEnvelopeAttributes loaderEnvelopeAttributes) {
    this.loaderEnvelopeAttributes = loaderEnvelopeAttributes;
  }

  public LoadRequest getLoadRequest() {
    return loadRequest;
  }

  public void setLoadRequest(LoadRequest loadRequest) {
    this.loadRequest = loadRequest;
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    LoaderEnvelope that = (LoaderEnvelope) o;
    return Objects.equal(getLoaderEnvelopeAttributes(), that.getLoaderEnvelopeAttributes())
        && Objects.equal(getLoadRequest(), that.getLoadRequest());
  }

  @Override public int hashCode() {
    return Objects.hashCode(getLoaderEnvelopeAttributes(), getLoadRequest());
  }

  @Override public String toString() {
    return "LoaderEnvelope{" + "loaderEnvelopeAttributes=" + loaderEnvelopeAttributes
        + ", loadRequest=" + loadRequest + '}';
  }
}
