package io.tapdata.entity.mapping.type;

/**
 * "datetime": {"range": ["1000-01-01 00:00:00", "9999-12-31 23:59:59"], "to": "typeDateTime"},
 */
public class TapDateTimeMapping extends TapDateBase {

    @Override
    protected String pattern() {
        return "yyyy-MM-dd HH:mm:ss";
    }

}
