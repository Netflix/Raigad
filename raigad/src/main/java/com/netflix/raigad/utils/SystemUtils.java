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
package com.netflix.raigad.utils;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;


public class SystemUtils
{
    private static final Logger logger = LoggerFactory.getLogger(SystemUtils.class);
    public static final String NOT_FOUND_STR = "NOT_FOUND";

    public static String getDataFromUrl(String url) {
        try
        {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(1000);
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() == 404)
            {
                return NOT_FOUND_STR;
            }
            if (conn.getResponseCode() != 200)
            {
                throw new RuntimeException("Unable to get data for URL " + url);
            }
            byte[] b = new byte[2048];
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataInputStream d = new DataInputStream((FilterInputStream) conn.getContent());
            int c = 0;
            while ((c = d.read(b, 0, b.length)) != -1)
                bos.write(b, 0, c);
            String return_ = new String(bos.toByteArray(), Charsets.UTF_8);
            logger.info("Calling URL API: {} returns: {}", url, return_);
            conn.disconnect();
            return return_;
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }

    }

    public static String runHttpGetCommand(String url) throws Exception
    {
        String return_;
        DefaultHttpClient client = new DefaultHttpClient();
        InputStream isStream = null;
        try {
            HttpParams httpParameters = new BasicHttpParams();
            int timeoutConnection = 1000;
            int timeoutSocket = 1000;
            HttpConnectionParams.setConnectionTimeout(httpParameters, timeoutConnection);
            HttpConnectionParams.setSoTimeout(httpParameters, timeoutSocket);
            client.setParams(httpParameters);

            HttpGet getRequest = new HttpGet(url);
            getRequest.setHeader("Content-type", "application/json");

            HttpResponse resp = client.execute(getRequest);

            if (resp == null || resp.getEntity() == null) {
                throw new ESHttpException("Unable to execute GET URL (" + url + ") Exception Message: < Null Response or Null HttpEntity >");
            }

            isStream = resp.getEntity().getContent();

            if (resp.getStatusLine().getStatusCode() != 200) {

                throw new ESHttpException("Unable to execute GET URL (" + url + ") Exception Message: (" + IOUtils.toString(isStream,StandardCharsets.UTF_8.toString()) + ")");
            }

            return_=IOUtils.toString(isStream,StandardCharsets.UTF_8.toString());
            logger.debug("GET URL API: {} returns: {}", url, return_);
        }
        catch(Exception e)
        {
            throw new ESHttpException("Caught an exception during execution of URL (" + url + ")Exception Message: (" + e + ")");
        }
        finally{
            if (isStream != null)
                isStream.close();
        }
        return return_;
    }

    public static String runHttpPutCommand(String url,String jsonBody) throws IOException
    {
        String return_;
        DefaultHttpClient client = new DefaultHttpClient();
        InputStream isStream = null;
        try {
            HttpParams httpParameters = new BasicHttpParams();
            int timeoutConnection = 1000;
            int timeoutSocket = 1000;
            HttpConnectionParams.setConnectionTimeout(httpParameters, timeoutConnection);
            HttpConnectionParams.setSoTimeout(httpParameters, timeoutSocket);
            client.setParams(httpParameters);

            HttpPut putRequest = new HttpPut(url);
            putRequest.setEntity(new StringEntity(jsonBody, StandardCharsets.UTF_8));
            putRequest.setHeader("Content-type", "application/json");

            HttpResponse resp = client.execute(putRequest);

            if (resp == null || resp.getEntity() == null) {
                throw new ESHttpException("Unable to execute PUT URL (" + url + ") Exception Message: < Null Response or Null HttpEntity >");
            }

            isStream = resp.getEntity().getContent();

            if (resp.getStatusLine().getStatusCode() != 200) {

                throw new ESHttpException("Unable to execute PUT URL (" + url + ") Exception Message: (" + IOUtils.toString(isStream,StandardCharsets.UTF_8.toString()) + ")");
            }

            return_=IOUtils.toString(isStream,StandardCharsets.UTF_8.toString());
            logger.debug("PUT URL API: {} with JSONBody {} returns: {}", url, jsonBody, return_);
        }
        catch(Exception e)
        {
            throw new ESHttpException("Caught an exception during execution of URL (" + url + ")Exception Message: (" + e + ")");
        }
        finally{
            if (isStream != null)
                isStream.close();
        }
        return return_;
    }

    public static String runHttpPostCommand(String url,String jsonBody) throws IOException
    {
        String return_;
        DefaultHttpClient client = new DefaultHttpClient();
        InputStream isStream = null;
        try {
            HttpParams httpParameters = new BasicHttpParams();
            int timeoutConnection = 1000;
            int timeoutSocket = 1000;
            HttpConnectionParams.setConnectionTimeout(httpParameters, timeoutConnection);
            HttpConnectionParams.setSoTimeout(httpParameters, timeoutSocket);
            client.setParams(httpParameters);

            HttpPost postRequest = new HttpPost(url);
            if(StringUtils.isNotEmpty(jsonBody))
                postRequest.setEntity(new StringEntity(jsonBody, StandardCharsets.UTF_8));
            postRequest.setHeader("Content-type", "application/json");

            HttpResponse resp = client.execute(postRequest);

            if (resp == null || resp.getEntity() == null) {
                throw new ESHttpException("Unable to execute POST URL (" + url + ") Exception Message: < Null Response or Null HttpEntity >");
            }

            isStream = resp.getEntity().getContent();

            if (resp.getStatusLine().getStatusCode() != 200) {

                throw new ESHttpException("Unable to execute POST URL (" + url + ") Exception Message: (" + IOUtils.toString(isStream,StandardCharsets.UTF_8.toString()) + ")");
            }

            return_=IOUtils.toString(isStream,StandardCharsets.UTF_8.toString());
            logger.debug("POST URL API: {} with JSONBody {} returns: {}", url, jsonBody, return_);
        }
        catch(Exception e)
        {
            throw new ESHttpException("Caught an exception during execution of URL (" + url + ")Exception Message: (" + e + ")");
        }
        finally{
            if (isStream != null)
                isStream.close();
        }
        return return_;
    }

    /**
     * delete all the files/dirs in the given Directory but dont delete the dir
     * itself.
     */
    public static void cleanupDir(String dirPath, List<String> childdirs) throws IOException
    {
        if (childdirs == null || childdirs.size() == 0)
            FileUtils.cleanDirectory(new File(dirPath));
        else
        {
            for (String cdir : childdirs)
                FileUtils.cleanDirectory(new File(dirPath + "/" + cdir));
        }
    }

    public static void createDirs(String location)
    {
        File dirFile = new File(location);
        if (dirFile.exists() && dirFile.isFile())
        {
            dirFile.delete();
            dirFile.mkdirs();
        }
        else if (!dirFile.exists())
            dirFile.mkdirs();
    }

    public static byte[] md5(byte[] buf)
    {
        try
        {
            MessageDigest mdigest = MessageDigest.getInstance("MD5");
            mdigest.update(buf, 0, buf.length);
            return mdigest.digest();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a Md5 string which is similar to OS Md5sum
     */
    public static String md5(File file)
    {
        try
        {
            HashCode hc = Files.hash(file, Hashing.md5());
            return toHex(hc.asBytes());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String toHex(byte[] digest)
    {
        StringBuffer sb = new StringBuffer(digest.length * 2);
        for (int i = 0; i < digest.length; i++)
        {
            String hex = Integer.toHexString(digest[i]);
            if (hex.length() == 1)
            {
                sb.append("0");
            }
            else if (hex.length() == 8)
            {
                hex = hex.substring(6);
            }
            sb.append(hex);
        }
        return sb.toString().toLowerCase();
    }

    public static String toBase64(byte[] md5)
    {
        byte encoded[] = Base64.encodeBase64(md5, false);
        return new String(encoded);
    }

    public static String formatDate(DateTime dateTime,String dateFormat)
    {
        DateTimeFormatter fmt = DateTimeFormat.forPattern(dateFormat);
        return dateTime.toString(fmt);
    }

    public static String[] getSecurityGroupIds(String MAC_ID)
    {
        String securityGroupIds = SystemUtils.getDataFromUrl(
                "http://169.254.169.254/latest/meta-data/network/interfaces/macs/" + MAC_ID +
                        "/security-group-ids/").trim();

        if (securityGroupIds.isEmpty()) {
            throw new RuntimeException("Security group ID's are null or empty");
        }

        return securityGroupIds.split("\n");
    }
}
