/*
#
# Copyright (C) 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
*/

package com.example;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.daead.DeterministicAeadConfig;
import com.google.crypto.tink.daead.DeterministicAeadFactory;
import com.google.crypto.tink.daead.DeterministicAeadKeyTemplates;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Security;

/**
 * This Utility Class can be used to a) Deterministically Encrypt the Sensitive PII Data and Encode
 * the resultant ByteArray to Base64 Encoded String b) Convert sensitive data values to sha256, and
 * storing the hash mapping in another highly restricted mapping table. If there is no need for
 * extracting the original string value, then just hashing would suffice
 */
public class EncryptUtils {

  private KeysetHandle keysetHandle;

  public EncryptUtils() {
    try {
      // Set the Crypto Policy to Unlimited(Needs Latest JDK 8)
      Security.setProperty("crypto.policy", "unlimited");
      DeterministicAeadConfig.register();
      this.keysetHandle = KeysetHandle.generateNew(DeterministicAeadKeyTemplates.AES256_SIV);
    } catch (GeneralSecurityException e) {
      e.printStackTrace();
    }
  }

  public byte[] encrypt(String data, String associatedData) throws GeneralSecurityException {
    return DeterministicAeadFactory.getPrimitive(this.keysetHandle)
        .encryptDeterministically(data.getBytes(), associatedData.getBytes());
  }

  public String decrypt(byte[] data, String associatedData) throws GeneralSecurityException {
    return new String(
        DeterministicAeadFactory.getPrimitive(this.keysetHandle)
            .decryptDeterministically(data, associatedData.getBytes()));
  }

  private String hash(String data) {
    return Hashing.sha256().newHasher().putString(data, StandardCharsets.UTF_8).hash().toString();
  }

  private String encode(byte[] data) {
    return BaseEncoding.base64Url().omitPadding().encode(data);
  }

  private byte[] decode(String data) {
    return BaseEncoding.base64Url().omitPadding().decode(data);
  }

  public String deSensitize(String data) {
    return hash(data);
  }

  public String deSensitize(String data, String associatedData) throws GeneralSecurityException {
    return encode(encrypt(data, associatedData));
  }

  public String extractSensitiveData(String data, String associatedData)
      throws GeneralSecurityException {
    return decrypt(decode(data), associatedData);
  }
}
