/**
 * Copyright 2016 Netflix, Inc.
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

import org.elasticsearch.common.logging.ESLogger;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class DataFetcher {
    public static String fetchData(String url, ESLogger logger) {
        DataInputStream responseStream = null;

        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(10000);
            conn.setRequestMethod("GET");

            if (conn.getResponseCode() != 200) {
                logger.error("Unable to get data from URL " + url);
                throw new RuntimeException("Unable to fetch data from Raigad API");
            }

            byte[] b = new byte[2048];
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            responseStream = new DataInputStream((FilterInputStream) conn.getContent());

            int c;
            while ((c = responseStream.read(b, 0, b.length)) != -1) {
                bos.write(b, 0, c);
            }

            String result = new String(bos.toByteArray(), StandardCharsets.UTF_8);
            logger.info("Calling Raigad API ({}) returns {}", url, result);
            conn.disconnect();

            return result;
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        finally {
            try
            {
                if (responseStream != null) {
                    responseStream.close();
                }
            }
            catch (Exception e) {
                logger.warn("Failed to close response stream from Raigad", e);
            }
        }
    }
}
