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
package com.netflix.elasticcar.indexmanagement.indexfilters;

import com.netflix.elasticcar.indexmanagement.IIndexNameFilter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

/**
 * Courtesy: Jae Bae
 */
public class DailyIndexNameFilter implements IIndexNameFilter {
    public static final String id = "daily";

    @Override
    public boolean filter(String name) {
        if (name.length() < 9) {
            return false;
        }
        String date = name.substring(name.length() - 8, name.length());
        try {
            DateTime.parse(date, DateTimeFormat.forPattern("YYYYMMdd"));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String getNamePart(String name) {
        return name.substring(0, name.length() - 8);
    }

    @Override
    public String getId() {
        return id;
    }
}

