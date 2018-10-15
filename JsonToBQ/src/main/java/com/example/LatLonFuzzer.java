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

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Random;

public class LatLonFuzzer {
  private static final int EARTH_RADIUS_METERS = 6378000;
  private static final Random rand = new Random(System.nanoTime());

  private static double fuzz(int n) {
    return rand.nextBoolean() ? rand.nextDouble() * n : rand.nextDouble() * n * (-1);
  }

  private static double roundCDecimal(double n) {
    DecimalFormat df = new DecimalFormat("#.####");
    df.setRoundingMode(RoundingMode.CEILING);
    return Double.valueOf(df.format(n));
  }

  public static double fuzzLatitude(double lat, int meters) {
    double dx = fuzz(meters);
    return roundCDecimal(lat + (dx / EARTH_RADIUS_METERS) * (180 / Math.PI));
  }

  public static double fuzzLongitude(double lat, double lon, int meters) {
    double dx = fuzz(meters);
    return roundCDecimal(
        lon + (dx / EARTH_RADIUS_METERS) * (180 / Math.PI) / Math.cos(lat * Math.PI / 180));
  }

  public static LatLon fuzzLatLon(double lat, double lon, int meters) {
    return LatLon.of(fuzzLatitude(lat, meters), fuzzLongitude(lat, lon, meters));
  }

  public static LatLon fuzzLatLon(LatLon latlon, int meters) {
    return LatLon.of(
        fuzzLatitude(latlon.latitude, meters),
        fuzzLongitude(latlon.latitude, latlon.longitude, meters));
  }

  public static class LatLon {
    public final double latitude;
    public final double longitude;

    private LatLon(double lat, double lon) {
      this.latitude = lat;
      this.longitude = lon;
    }

    public static LatLon of(double lat, double lon) {
      return new LatLon(lat, lon);
    }
  }
}
