/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.raigad.discovery.utils;

import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class DataFetcher {
    public static String fetchData(String url, Logger logger) {
        HttpURLConnection httpConnection = null;
        DataInputStream responseStream = null;

        try {
            httpConnection = (HttpURLConnection) new URL(url).openConnection();
            httpConnection.setConnectTimeout(1000);
            httpConnection.setReadTimeout(10000);
            httpConnection.setRequestMethod("GET");

            if (httpConnection.getResponseCode() != 200) {
                logger.error("Unable to get data from URL [" + url + "]");
                throw new RuntimeException("Unable to fetch data from Raigad API");
            }

            byte[] bytes = new byte[2048];
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            responseStream = new DataInputStream((FilterInputStream) httpConnection.getContent());

            int bytesRead;
            while ((bytesRead = responseStream.read(bytes, 0, bytes.length)) != -1) {
                byteArrayOutputStream.write(bytes, 0, bytesRead);
            }

            String result = new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8);
            logger.info("Raigad ({}) returned {}", url, result);

            return result;

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            try {
                if (responseStream != null) {
                    responseStream.close();
                }
            } catch (Exception e) {
                logger.warn("Failed to close response stream from Raigad", e);
            }

            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }
}
