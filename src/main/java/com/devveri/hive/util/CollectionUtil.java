package com.devveri.hive.util;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

public final class CollectionUtil {

    public static Set<String> diff(Collection<String> source, Collection<String> target) {
        Set<String> set = new TreeSet<>();
        for (String str : source) {
            if (!target.contains(str)) {
                set.add(str);
            }
        }
        return set;
    }

}
