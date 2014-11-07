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
package com.netflix.raigad.indexmanagement;

import java.util.Comparator;

/**
 * Courtesy Jae Bae
 */
class IndexPriority implements Comparator<String> {
    @Override
    public int compare(String o1, String o2) {
        String name1 = o1.substring(0, o1.length() - 8);
        int date1 = Integer.parseInt(o1.substring(o1.length() - 8));

        String name2 = o2.substring(0, o2.length() - 8);
        int date2 = Integer.parseInt(o2.substring(o2.length() - 8));

        int r = date1 - date2;
        if (r != 0) {
            return r;
        } else {
            return name1.compareTo(name2);
        }
    }
}