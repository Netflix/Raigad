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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class YearlyIndexNameFilter implements IIndexNameFilter {

    public static final String id = "yearly";
    String YEARLY_PATTERN = "(\\w)+[[a-zA-Z]]{1}[0-9]{4}";

    @Override
    public boolean filter(String name) {
        if (name.length() < 5) {
            return false;
        }

        Pattern pattern = Pattern.compile(YEARLY_PATTERN);
        Matcher matcher = pattern.matcher(name);
        if(!matcher.matches())
            return false;

        String date = name.substring(name.length() - 4, name.length());
        try {
            DateTime.parse(date, DateTimeFormat.forPattern("YYYY"));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String getNamePart(String name) {
        return name.substring(0, name.length() - 4);
    }

    @Override
    public String getId() {
        return id;
    }
}
