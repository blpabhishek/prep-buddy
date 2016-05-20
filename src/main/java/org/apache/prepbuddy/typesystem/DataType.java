package org.apache.prepbuddy.typesystem;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

public enum DataType implements Serializable {
    INTEGER {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.INT_PATTERN, sampleData);
        }
    }, URL {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.URL_PATTERN, sampleData);
        }
    }, EMAIL {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.EMAIL_PATTERN, sampleData);
        }
    }, CURRENCY {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.CURRENCY_PATTERN, sampleData);
        }
    }, DECIMAL {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.DECIMAL_PATTERN, sampleData);
        }
    }, SOCIAL_SECURITY_NUMBER {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.SSN_PATTERN, sampleData);
        }
    }, IP_ADDRESS {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.IP_PATTERN, sampleData);
        }
    }, ZIP_CODE {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.ZIP_PATTERN, sampleData);
        }
    }, ALPHANUMERIC_STRING {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return false;
        }
    }, COUNTRY_CODE {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return isCountry(sampleData);
        }
    };

    final String INT_PATTERN = "^\\d+$";
    final String DECIMAL_PATTERN = "^\\.\\d+|\\d+\\.\\d+$";
    final String URL_PATTERN = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]$";
    final String EMAIL_PATTERN = "^[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
    final String CURRENCY_PATTERN = "^(\\p{Sc})(\\d+|\\d+.\\d+)$";
    final String SSN_PATTERN = "^(\\d{3}-\\d{2}-\\d{4})$";
    final String IP_PATTERN = "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
    final String ZIP_PATTERN = "^[0-9]{5}(?:-[0-9]{4})?$";

    boolean isCountry(List<String> sampleData) {
        String[] listOfCountry = Locale.getISOCountries();
        Set<String> sample = new TreeSet<>(sampleData);
        int size = listOfCountry.length;
        for (String country : listOfCountry) sample.add(country);
        return (sample.size() == size);
    }

    public boolean matchesWith(String regex, List<String> samples) {
        int counter = 0;
        int threshold = samples.size() / 2;
        for (String string : samples)
            if (string.matches(regex)) {
                counter++;
            }
        return (counter >= threshold);
    }

    public abstract boolean isOfType(List<String> sampleData);
}
