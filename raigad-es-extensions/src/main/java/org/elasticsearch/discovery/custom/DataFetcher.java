/**
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.discovery.custom;

import org.elasticsearch.common.logging.ESLogger;
import org.apache.commons.lang.CharEncoding;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class DataFetcher {

    public static String fetchData(String url, ESLogger logger) {
        DataInputStream responseStream = null;
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(10000);
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() != 200)
                throw new RuntimeException("Unable to get data for URL " + url);

            byte[] b = new byte[2048];
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            responseStream = new DataInputStream((FilterInputStream) conn.getContent());
            int c = 0;
            while ((c = responseStream.read(b, 0, b.length)) != -1)
                bos.write(b, 0, c);
            String return_ = new String(bos.toByteArray(), CharEncoding.UTF_8);
            logger.info(String.format("Calling URL API: %s returns: %s", url, return_));
            conn.disconnect();
            return return_;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            try {
                if (responseStream != null)
                    responseStream.close();
            } catch (Exception e) {
                logger.warn("Failed to close response stream from priam", e);
            }
        }
    }

}
