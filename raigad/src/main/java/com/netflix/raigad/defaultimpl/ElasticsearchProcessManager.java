/**
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.raigad.defaultimpl;

import com.google.inject.Inject;
import com.netflix.raigad.configuration.IConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

public class ElasticsearchProcessManager implements IElasticsearchProcess {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchProcessManager.class);
    private static final int SCRIPT_EXECUTE_WAIT_TIME_MS = 5000;
    private final IConfiguration config;

    @Inject
    public ElasticsearchProcessManager(IConfiguration config) {
        this.config = config;
    }

    String getStartupCommand() {
        return StringUtils.trimToEmpty(config.getElasticsearchStartupScript());
    }

    String getStopCommand() {
        return StringUtils.trimToEmpty(config.getElasticsearchStopScript());
    }

    void runCommand(String command) {
        Process process = null;

        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command).redirectErrorStream(true);
            process = processBuilder.start();

            process.waitFor(SCRIPT_EXECUTE_WAIT_TIME_MS, TimeUnit.MILLISECONDS);

            int exitCode = process.exitValue();
            if (exitCode == 0) {
                logger.info(String.format("Successfully executed %s", command));
            } else {
                logger.error(String.format("Error executing %s, exited with code %d", command, exitCode));
            }
        } catch (Exception e) {
            logger.error(String.format("Exception executing %s", command), e);
        } finally {
            if (process != null) {
                process.destroyForcibly();
            }
        }
    }

    public void start() {
        logger.info("Starting Elasticsearch server");

        String startupCommand = getStartupCommand();

        if (startupCommand.isEmpty()) {
            logger.warn("Elasticsearch startup command was not specified");
            return;
        }

        runCommand(startupCommand);
    }

    public void stop() {
        logger.info("Stopping Elasticsearch server");

        String stopCommand = getStopCommand();

        if (stopCommand.isEmpty()) {
            logger.warn("Elasticsearch stop command was not specified");
            return;
        }

        runCommand(stopCommand);
    }

    void logProcessOutput(Process process) {
        InputStream inputStream = null;

        try {
            inputStream = process.getInputStream();
            final String processOutputStream = readProcessStream(inputStream);
            logger.info("Standard/Error out: {}", processOutputStream);
        } catch (IOException e) {
            logger.warn("Failed to read the standard/error output stream", e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    logger.warn("Failed to close the standard/error output stream", e);
                }
            }
        }
    }

    private String readProcessStream(InputStream inputStream) throws IOException {
        final byte[] buffer = new byte[512];
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(buffer.length);
        int count;

        while ((count = inputStream.read(buffer)) != -1) {
            byteArrayOutputStream.write(buffer, 0, count);
        }

        return byteArrayOutputStream.toString();
    }
}
