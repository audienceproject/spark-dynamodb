/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  *
  * Copyright © 2019 AudienceProject. All rights reserved.
  */
package com.audienceproject.spark.dynamodb.util

// NOT THREAD SAFE
class RateLimiter(capacity: Double) {

    private var pressure = 0d
    private var lastEvent = System.currentTimeMillis()

    def acquire(amount: Double): Unit = {
        pressure = (pressure - decay()) max 0
        pressure += amount
        lastEvent = System.currentTimeMillis()
        if (pressure > capacity) {
            Thread.sleep(delay().toLong)
        }
    }

    private def decay(): Double = (System.currentTimeMillis() - lastEvent) / 1000 * capacity

    private def delay(): Double = (pressure - capacity) / capacity * 1000

}
