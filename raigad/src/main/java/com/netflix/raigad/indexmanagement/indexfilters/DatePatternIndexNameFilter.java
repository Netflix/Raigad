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
package com.netflix.raigad.indexmanagement.indexfilters;

import com.netflix.raigad.indexmanagement.IIndexNameFilter;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormatter;

public class DatePatternIndexNameFilter implements IIndexNameFilter {

    private final DateTimeFormatter formatter;

    public DatePatternIndexNameFilter(DateTimeFormatter formatter) {
        this.formatter = formatter;
    }

    @Override
    public boolean filter(String name) {
        try {
            MutableDateTime instant = new MutableDateTime();
            int pos = formatter.parseInto(instant, name, 0);
            return pos > 0
                && pos == name.length()
                && checkYear(instant)
                && reproducible(name, instant);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private boolean checkYear(MutableDateTime instant) {
        // When using a pattern like YYYY, it will match strings like 201802 as a large
        // year. For our use-cases this is more likely a separate index with a year and
        // month pattern. To avoid this the year is checked and rejected if more than four
        // digits.
        return instant.getYear() < 10000;
    }

    private boolean reproducible(String expected, MutableDateTime instant) {
        // The date time parser is sometimes more lenient for parsing than what it would
        // be able to generate. For example a pattern like YYYYMM would match both 20131
        // and 201301. This check ensures that the printed form matches. So for the example
        // 20131 would not match, but 201301 would.
        String actual = formatter.print(instant);
        return actual.equals(expected);
    }
}
