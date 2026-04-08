const roundCurrency = (value) => Math.round((Number(value || 0) + Number.EPSILON) * 100) / 100;

const toFiniteNumber = (value) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
};

export const calculatePushServiceCharge = (oldServiceCharge, increasePct) => {
    const oldCharge = toFiniteNumber(oldServiceCharge);
    const pct = toFiniteNumber(increasePct);

    if (oldCharge == null || oldCharge <= 0 || pct == null) {
        return 0;
    }

    return roundCurrency(oldCharge * (1 + (pct / 100)));
};

export const calculatePushArv = ({ oldArv, annualIncrease, increasePct }) => {
    const currentArv = toFiniteNumber(oldArv);
    if (currentArv == null || currentArv <= 0) {
        return 0;
    }

    const annualDelta = toFiniteNumber(annualIncrease);
    if (annualDelta != null && annualDelta !== 0) {
        return roundCurrency(currentArv + annualDelta);
    }

    const pct = toFiniteNumber(increasePct);
    if (pct == null) {
        return roundCurrency(currentArv);
    }

    return roundCurrency(currentArv * (1 + (pct / 100)));
};
