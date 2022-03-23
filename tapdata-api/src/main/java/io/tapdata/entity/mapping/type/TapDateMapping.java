package io.tapdata.entity.mapping.type;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * "date": {"range": ["1000-01-01", "9999-12-31"], "gmt" : 0, "to": "typeDate"},
 */
public class TapDateMapping extends TapDateBase {

    @Override
    protected String pattern() {
        return "yyyy-MM-dd";
    }

}
