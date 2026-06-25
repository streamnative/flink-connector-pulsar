/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.common.config;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PulsarClientFactory}. */
class PulsarClientFactoryTest {

    @Test
    void getAdminUrlPulsarSchemeConvertsToHttp() throws Exception {
        assertThat(invokeGetAdminUrl("pulsar://localhost:6650")).isEqualTo("http://localhost:8080");
    }

    @Test
    void getAdminUrlPulsarSslSchemeConvertsToHttps() throws Exception {
        assertThat(invokeGetAdminUrl("pulsar+ssl://localhost:6651"))
                .isEqualTo("https://localhost:8443");
    }

    @Test
    void getAdminUrlPulsarSchemeWithCustomPortPreserved() throws Exception {
        assertThat(invokeGetAdminUrl("pulsar://example.com:9999"))
                .isEqualTo("http://example.com:9999");
    }

    @Test
    void getAdminUrlPulsarSslSchemeWithCustomPortPreserved() throws Exception {
        assertThat(invokeGetAdminUrl("pulsar+ssl://example.com:7777"))
                .isEqualTo("https://example.com:7777");
    }

    @Test
    void getAdminUrlNonPulsarSchemePassedThrough() throws Exception {
        assertThat(invokeGetAdminUrl("http://example.com:8080"))
                .isEqualTo("http://example.com:8080");
    }

    @Test
    void getAdminUrlHttpsSchemePassedThrough() throws Exception {
        assertThat(invokeGetAdminUrl("https://example.com:8443"))
                .isEqualTo("https://example.com:8443");
    }

    @Test
    void getAdminUrlFirstOfMultipleUrlsUsed() throws Exception {
        assertThat(invokeGetAdminUrl("pulsar://broker1:6650, pulsar://broker2:6650"))
                .isEqualTo("http://broker1:8080");
    }

    @Test
    void getAdminUrlWithPathAndQueryPreserved() throws Exception {
        assertThat(invokeGetAdminUrl("pulsar://localhost:6650/some/path?key=value"))
                .isEqualTo("http://localhost:8080/some/path?key=value");
    }

    private static String invokeGetAdminUrl(String serviceUrl) throws Exception {
        Method method = PulsarClientFactory.class.getDeclaredMethod("getAdminUrl", String.class);
        method.setAccessible(true);
        return (String) method.invoke(null, serviceUrl);
    }
}
