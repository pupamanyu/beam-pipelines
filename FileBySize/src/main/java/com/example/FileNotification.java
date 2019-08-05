package com.example;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

@AutoValue
@JsonSerialize(as = FileNotification.class)
@JsonDeserialize(builder = FileNotification.Builder.class)
public abstract class FileNotification implements Serializable {

  /*
   * TODO: Make FileNotification AvroCoder Compatible
   */

  @NotNull
  public static Builder builder() {
    return Builder.builder();
  }

  @NotNull
  @JsonProperty("kind")
  public abstract String kind();

  @NotNull
  @JsonProperty("id")
  public abstract String id();

  @NotNull
  @JsonProperty("selfLink")
  public abstract String selfLink();

  @NotNull
  @JsonProperty("name")
  public abstract String name();

  @NotNull
  @JsonProperty("bucket")
  public abstract String bucket();

  @NotNull
  @JsonProperty("generation")
  public abstract String generation();

  @NotNull
  @JsonProperty("metageneration")
  public abstract String metageneration();

  @NotNull
  @JsonProperty("contentType")
  public abstract String contentType();

  @NotNull
  @JsonProperty("timeCreated")
  public abstract String timeCreated();

  @NotNull
  @JsonProperty("updated")
  public abstract String updated();

  @NotNull
  @JsonProperty("storageClass")
  public abstract String storageClass();

  @NotNull
  @JsonProperty("timeStorageClassUpdated")
  public abstract String timeStorageClassUpdated();

  @NotNull
  @JsonProperty("size")
  public abstract Long size();

  @NotNull
  @JsonProperty("md5Hash")
  public abstract String md5Hash();

  @NotNull
  @JsonProperty("mediaLink")
  public abstract String mediaLink();

  @NotNull
  @JsonProperty("contentLanguage")
  public abstract String contentLanguage();

  @NotNull
  @JsonProperty("crc32c")
  public abstract String crc32c();

  @NotNull
  @JsonProperty("etag")
  public abstract String etag();

  @NotNull
  @AutoValue.Builder
  public abstract static class Builder {

    @JsonCreator
    public static Builder builder() {
      return new AutoValue_FileNotification.Builder();
    }

    @JsonProperty("kind")
    public abstract Builder kind(@NotNull String kind);

    @JsonProperty("id")
    public abstract Builder id(@NotNull String id);

    @JsonProperty("selfLink")
    public abstract Builder selfLink(@NotNull String selfLink);

    @JsonProperty("name")
    public abstract Builder name(@NotNull String name);

    @JsonProperty("bucket")
    public abstract Builder bucket(@NotNull String bucket);

    @JsonProperty("generation")
    public abstract Builder generation(@NotNull String generation);

    @JsonProperty("metageneration")
    public abstract Builder metageneration(@NotNull String metageneration);

    @JsonProperty("contentType")
    public abstract Builder contentType(@NotNull String contentType);

    @JsonProperty("timeCreated")
    public abstract Builder timeCreated(@NotNull String timeCreated);

    @JsonProperty("updated")
    public abstract Builder updated(@NotNull String updated);

    @JsonProperty("storageClass")
    public abstract Builder storageClass(@NotNull String storageClass);

    @JsonProperty("timeStorageClassUpdated")
    public abstract Builder timeStorageClassUpdated(@NotNull String timeStorageClassUpdated);

    @JsonProperty("size")
    public abstract Builder size(@NotNull Long size);

    @JsonProperty("md5Hash")
    public abstract Builder md5Hash(@NotNull String md5Hash);

    @JsonProperty("mediaLink")
    public abstract Builder mediaLink(@NotNull String mediaLink);

    @JsonProperty("contentLanguage")
    public abstract Builder contentLanguage(@NotNull String contentLanguage);

    @JsonProperty("crc32c")
    public abstract Builder crc32c(@NotNull String crc32c);

    @JsonProperty("etag")
    public abstract Builder etag(@NotNull String etag);

    public abstract FileNotification build();
  }
}
