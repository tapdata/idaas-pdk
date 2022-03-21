package io.tapdata.entity.mapping.type;

/**
 * "time": {"range": ["-838:59:59","838:59:59"], "to": "typeInterval:typeNumber"},
 */
public class TapTimeMapping extends TapDateBase {

    @Override
    protected String pattern() {
        return "HH:mm:ss";
    }

}
