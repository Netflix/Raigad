package com.netflix.raigad.dataobjects;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by alfasi on 4/15/15.
 *
 * NodeHealthChecker verifies that Elasticsearch process is running on the instance
 * In case ES is up it should return the status 200 (Integer), otherwise it returns 500
 */
@Singleton
public class NodeHealthChecker {

    private static final Logger logger = LoggerFactory.getLogger(NodeHealthChecker.class);
    private final IConfiguration config;

    @Inject
    public NodeHealthChecker(IConfiguration config) {
        this.config = config;
    }

    public int isEsUpOnInstance() {
        return parseResponse(getStatusWithRetries(1));
    }

    protected int parseResponse(String response) {
        Pattern pattern = Pattern.compile("status.*?: (.*?),");
        Matcher matcher = pattern.matcher(response);
        String match = "500";
        if (matcher.find()) {
            match = matcher.group(1).trim();
        }
        return Integer.valueOf(match);
    }

    private String getStatusWithRetries
            (int retries) {
        if (retries > 4) {
            return  "";
        }
        String url = "http://127.0.0.1:" + config.getHttpPort();
        String resp = "";
        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("GET");
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                resp += inputLine;
            }
            in.close();
        } catch (Exception e) {
            // sometimes we experience transient network errors
            // that doesn't mean we need to alert - just try again
            logger.error(e.getMessage());
            try {
                Thread.sleep(3000 * retries);
            }
            catch (InterruptedException e1) {
                // ignore
            }
            finally {
                getStatusWithRetries(retries + 1);
            }
        }
        logger.info("getStatusWithRetries() returned: " + resp);
        return resp;
    }
}
