package com.netflix.elasticcar.indexmanagement;

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