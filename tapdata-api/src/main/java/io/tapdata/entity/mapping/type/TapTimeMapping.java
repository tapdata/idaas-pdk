package io.tapdata.entity.mapping.type;

import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * "time": {"range": ["-838:59:59","838:59:59"], "to": "typeInterval:typeNumber"},
 */
public class TapTimeMapping extends TapDateBase {

    @Override
    protected String pattern() {
        return "HH:mm:ss";
    }

    @Override
    public TapType toTapType(String dataType, Map<String, String> params) {
        return new TapTime();
    }

    @Override
    public TapResult<String> fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapTime) {
            return TapResult.successfully(removeBracketVariables(typeExpression, 0));
        }
        return null;
    }
    final BigDecimal rangeValue = BigDecimal.valueOf(10).pow(19);
    final BigDecimal timeZoneValue = BigDecimal.valueOf(10).pow(17);
    @Override
    public BigDecimal matchingScore(TapField field) {
        if (field.getTapType() instanceof TapTime) {
            TapTime tapTime = (TapTime) field.getTapType();

            //field is primary key, but this type is not able to be primary type.
            if(field.getPrimaryKey() != null && field.getPrimaryKey() && pkEnablement != null && !pkEnablement) {
                return BigDecimal.valueOf(-Double.MAX_VALUE);
            }

            BigDecimal score = BigDecimal.ZERO;

            Boolean withTimeZone = tapTime.getWithTimeZone();

            Instant max = tapTime.getMax();
            Instant min = tapTime.getMin();

            if((withTimeZone != null && withTimeZone && this.withTimeZone != null && this.withTimeZone) ||
                    ((withTimeZone == null || !withTimeZone) && (this.withTimeZone == null || !this.withTimeZone))) {
                score = score.add(timeZoneValue);
            } else {
                score = score.subtract(timeZoneValue);
            }

            if(min != null && max != null && this.min != null && this.max != null)
                score = score.add(calculateScoreForValue(min, max, this.min, this.max, rangeValue));

            return score;
        }

        return BigDecimal.valueOf(-Double.MAX_VALUE);
    }
}
