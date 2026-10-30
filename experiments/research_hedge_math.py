"""Illustrative quote-based hedging mathematics; no final research specification.

Black forward values use zero discounting (r=0). SM means sticky moneyness,
not an unspecified universal BSAD. Inverse hedge conversion assumes parallel
percentage movements of pricing forward F, conversion index S and hedge H.
One-period risk is gross, in coin: dq - n * (1 - H_t / H_u), with n=N/H_t.
Funding, fees, execution, collateral constraints and model validation are outside
this module. IV quote-band bounds are deterministic sensitivity bounds, not CIs.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import math

import numpy as np


class MathDomainError(ValueError):
    """Inputs cannot identify the requested mathematical quantity."""


def _positive(**values):
    for name, value in values.items():
        if not math.isfinite(float(value)) or value <= 0:
            raise MathDomainError(f"{name} must be finite and positive")


def _put(option_type):
    value = str(option_type).upper()
    if value not in {"C", "P", "CALL", "PUT"}:
        raise MathDomainError("option_type must be C, P, call or put")
    return value in {"P", "PUT"}


def _cdf(value):
    # erfc avoids cancellation in the negative tail.
    return 0.5 * math.erfc(-value / math.sqrt(2.0))


def black_greeks(F, K, tau, sigma, option_type="C"):
    """Return USD price, derivative w.r.t. F and vega per unit decimal IV.

    F,K are USD/coin; tau is years; sigma=.60 means 60% annual IV.
    Values are per one coin underlying and assume discount factor one.
    """
    _positive(F=F, K=K, tau=tau)
    if not math.isfinite(float(sigma)) or sigma < 0:
        raise MathDomainError("sigma must be finite and nonnegative")
    put = _put(option_type)
    if sigma == 0:
        delta = float(F > K) if F != K else 0.5
        vega = F*math.sqrt(tau)/math.sqrt(2*math.pi) if F == K else 0.0
        return {"price": max(K-F, 0.0) if put else max(F-K, 0.0),
                "delta_forward": delta - int(put), "vega": vega}
    st = sigma * math.sqrt(tau)
    d1 = math.log(F / K) / st + 0.5 * st
    d2 = d1 - st
    # Use OTM value + parity to reduce subtraction for deep ITM contracts.
    if F >= K:
        p = max(K * _cdf(-d2) - F * _cdf(-d1), 0.0)
        price = p if put else p + F - K
    else:
        c = max(F * _cdf(d1) - K * _cdf(d2), 0.0)
        price = c + K - F if put else c
    return {"price": price, "delta_forward": _cdf(d1) - int(put),
            "vega": F * math.exp(-0.5*d1*d1) / math.sqrt(2*math.pi) * math.sqrt(tau)}


def black_price(F, K, tau, sigma, option_type="C"):
    return black_greeks(F, K, tau, sigma, option_type)["price"]


def implied_volatility(price, F, K, tau, option_type="C", *,
                       max_vol=100.0, price_rtol=1e-12, max_iter=160):
    """Invert a USD Black price by bisection; reject arbitrage/domain errors.

    Exact intrinsic value maps to IV zero. An upper-bound price has no finite IV.
    Coin quotes must be multiplied by the contemporaneous conversion index first.
    """
    _positive(F=F, K=K, tau=tau, max_vol=max_vol)
    if not math.isfinite(float(price)) or price < 0:
        raise MathDomainError("price must be finite and nonnegative")
    put = _put(option_type)
    intrinsic = max(K-F, 0.0) if put else max(F-K, 0.0)
    upper = K if put else F
    if price < intrinsic:
        raise MathDomainError("price below zero-rate intrinsic bound")
    if price >= upper:
        raise MathDomainError("price at/above Black upper bound")
    if price == intrinsic:
        return 0.0
    lo, hi = 0.0, 1.0
    while hi < max_vol and black_price(F, K, tau, hi, option_type) < price:
        hi = min(hi * 2.0, max_vol)
    if black_price(F, K, tau, hi, option_type) < price:
        raise MathDomainError("IV exceeds max_vol")
    tolerance = price_rtol * max(F, K)
    for _ in range(max_iter):
        mid = (lo + hi) * 0.5
        value = black_price(F, K, tau, mid, option_type)
        if abs(value-price) <= tolerance:
            return mid
        if value < price:
            lo = mid
        else:
            hi = mid
    raise MathDomainError("IV bisection did not converge")


def implied_volatility_band(bid, ask, F, K, tau, option_type="C", **kwargs):
    """USD bid/ask input; fail rather than clip quotes violating model bounds."""
    if not (math.isfinite(float(bid)) and math.isfinite(float(ask))):
        raise MathDomainError("bid and ask must be finite")
    if bid <= 0 or ask < bid:
        raise MathDomainError("require positive bid and ask >= bid")
    return {"bid_iv": implied_volatility(bid, F, K, tau, option_type, **kwargs),
            "mid_iv": implied_volatility((bid+ask)/2, F, K, tau, option_type, **kwargs),
            "ask_iv": implied_volatility(ask, F, K, tau, option_type, **kwargs)}


def local_quadratic_smile(moneyness, iv, iv_bid, iv_ask, target_m):
    """Fit five distinct strikes, including two strictly either side of target.

    Prefer an exact target as fifth point; otherwise use the nearest remaining
    strike. Caller must deduplicate calls/puts at the same strike and ensure all
    data have one expiry and causal, fresh quotes. No extrapolation is allowed.
    Coefficients and slope weights use centered/scaled moneyness. The interval
    propagates independent per-strike IV bands through the SAME linear fit.
    """
    arrays = [np.asarray(x, dtype=float) for x in (moneyness, iv, iv_bid, iv_ask)]
    if any(a.ndim != 1 for a in arrays) or len({len(a) for a in arrays}) != 1:
        raise MathDomainError("smile inputs must be equal-length vectors")
    m, vol, low, high = arrays
    if len(m) < 5 or not all(np.isfinite(a).all() for a in arrays):
        raise MathDomainError("need at least five finite smile observations")
    if not math.isfinite(float(target_m)) or target_m <= 0 or np.any(m <= 0):
        raise MathDomainError("moneyness must be finite and positive")
    if len(np.unique(m)) != len(m):
        raise MathDomainError("deduplicate strike moneyness before fitting")
    if np.any(low < 0) or np.any(low > vol) or np.any(vol > high):
        raise MathDomainError("require 0 <= bid IV <= midpoint IV <= ask IV")
    eps = 1e-12 * max(1.0, abs(target_m))
    left = np.flatnonzero(m < target_m-eps)
    right = np.flatnonzero(m > target_m+eps)
    if len(left) < 2 or len(right) < 2:
        raise MathDomainError("target needs two distinct strikes on each side")
    sides = list(left[np.argsort(abs(m[left]-target_m))[:2]])
    sides += list(right[np.argsort(abs(m[right]-target_m))[:2]])
    remaining = np.array([i for i in range(len(m)) if i not in sides])
    center = int(remaining[np.argmin(abs(m[remaining]-target_m))])
    indices = np.array(sorted(sides+[center], key=lambda i: m[i]), dtype=int)
    dx = m[indices]-target_m
    scale = float(np.max(abs(dx)))
    design = np.column_stack([np.ones(5), dx/scale, (dx/scale)**2])
    inverse = np.linalg.pinv(design)
    if np.linalg.matrix_rank(design) != 3:
        raise MathDomainError("quadratic smile design is rank deficient")
    coefficients = inverse @ vol[indices]
    slope_weights = inverse[1] / scale
    slope = float(slope_weights @ vol[indices])
    slope_low = float(np.where(slope_weights >= 0,
                              slope_weights*low[indices], slope_weights*high[indices]).sum())
    slope_high = float(np.where(slope_weights >= 0,
                               slope_weights*high[indices], slope_weights*low[indices]).sum())
    if coefficients[0] <= 0:
        raise MathDomainError("fitted target volatility is not positive")
    return {"indices": indices.tolist(), "iv_at_target": float(coefficients[0]),
            "slope": slope, "slope_lower": slope_low, "slope_upper": slope_high,
            "slope_worstcase_abs_error": max(slope-slope_low, slope_high-slope),
            "slope_weights": slope_weights.tolist(),
            "rmse": float(np.sqrt(np.mean((design@coefficients-vol[indices])**2))),
            "condition_number": float(np.linalg.cond(design)),
            "strike_span": [float(m[indices[0]]), float(m[indices[-1]])]}


def smile_adjusted_delta(delta_forward, vega, slope_m, m, F):
    """Illustrative SM/SI delta: delta_forward - vega * m/F * dIV/dm."""
    _positive(m=m, F=F)
    if not all(math.isfinite(float(x)) for x in (delta_forward, vega, slope_m)):
        raise MathDomainError("Greeks and slope must be finite")
    return delta_forward - vega * m / F * slope_m


def inverse_hedge_normalized(F, delta, B, S):
    """n=N/H in coin for one option: (F*USD_delta - USD_option_value)/S.

    Assumes parallel percentage moves of F,S,H and fixed discounting.
    B may be observed q*S; track its mismatch with the BS fitted value externally.
    Finite-difference identity holds when B is the same model's value.
    """
    _positive(F=F, S=S)
    if not math.isfinite(float(delta)) or not math.isfinite(float(B)) or B < 0:
        raise MathDomainError("delta must be finite and B finite/nonnegative")
    return (F*delta-B)/S


@dataclass
class ResidualRidge:
    mean: np.ndarray
    scale: np.ndarray
    coefficients: np.ndarray
    alpha: float
    train_rows: int
    design_rank: int

    def predict(self, features):
        """Return the correction to normalized hedge n, not a price prediction."""
        x = np.asarray(features, dtype=float)
        if x.ndim != 2 or x.shape[1] != len(self.mean) or not np.isfinite(x).all():
            raise MathDomainError("prediction features have invalid shape or values")
        phi = np.column_stack([np.ones(len(x)), (x-self.mean)/self.scale])
        return phi @ self.coefficients


def fit_residual_ridge(features, residual_target, hedge_gain, alpha=1e-4,
                       sample_weight=None):
    """Training-only normalization and inverse-risk residual ridge.

    residual_target = dq - n_baseline*z, hedge_gain=z=1-H_t/H_u.
    Fits mean weighted (residual_target-z*(b0+standardized_x@b))**2
    + alpha*sum(b**2). The hedge correction b0 is inside z, not a free P&L
    intercept. Its coefficient is unpenalized. No division by hedge returns.
    Caller's responsibility: chronological partitions and alpha selection.
    """
    x = np.asarray(features, dtype=float)
    y = np.asarray(residual_target, dtype=float)
    z = np.asarray(hedge_gain, dtype=float)
    if x.ndim != 2 or y.shape != (len(x),) or z.shape != y.shape or len(x) < 2:
        raise MathDomainError("invalid training shapes or fewer than two rows")
    if not all(np.isfinite(a).all() for a in (x, y, z)) or alpha < 0 or not math.isfinite(alpha):
        raise MathDomainError("training inputs and alpha must be finite; alpha >= 0")
    weights = np.ones(len(x)) if sample_weight is None else np.asarray(sample_weight, dtype=float)
    if weights.shape != y.shape or not np.isfinite(weights).all() or np.any(weights < 0) or weights.sum() <= 0:
        raise MathDomainError("sample weights must be nonnegative with positive sum")
    if np.sum(weights*z*z) <= 0:
        raise MathDomainError("no observed hedge-gain variation")
    mean = np.average(x, axis=0, weights=weights)
    scale = np.sqrt(np.average((x-mean)**2, axis=0, weights=weights))
    scale[scale < 1e-12] = 1.0
    phi = np.column_stack([np.ones(len(x)), (x-mean)/scale])
    design = z[:, None]*phi
    sw = np.sqrt(weights/weights.sum())
    a, b = design*sw[:, None], y*sw
    penalty = np.eye(design.shape[1])*math.sqrt(alpha)
    penalty[0, 0] = 0.0
    # Augmented least squares avoids squaring the design's condition number.
    augmented_a = np.vstack([a, penalty])
    augmented_b = np.concatenate([b, np.zeros(design.shape[1])])
    coefficients = np.linalg.lstsq(augmented_a, augmented_b, rcond=None)[0]
    return ResidualRidge(mean, scale, coefficients, float(alpha), len(x),
                         int(np.linalg.matrix_rank(a)))


def self_check():
    """Meaningful algebra, IV inversion, quote-band and risk-regression checks."""
    roundtrip_errors = []
    for kind in ("C", "P"):
        for ratio in (0.9, 1.0, 1.1):
            for tau in (30/365, 180/365):
                for sigma in (0.4, 0.8, 1.2):
                    price = black_price(70000, ratio*70000, tau, sigma, kind)
                    recovered = implied_volatility(price, 70000, ratio*70000, tau, kind)
                    roundtrip_errors.append(abs(recovered-sigma))
    assert max(roundtrip_errors) < 1e-8
    for price in (-1, 70000, 80000):
        try:
            implied_volatility(price, 70000, 70000, 30/365)
        except MathDomainError:
            pass
        else:
            raise AssertionError("invalid IV domain accepted")
    fd_errors = []
    for kind in ("C", "P"):
        for K in (60000, 70000, 80000):
            H, tau = 70000.0, 20/365
            def volatility(h):
                m = K/h
                return 0.65-0.15*(m-1)+0.4*(m-1)**2
            def q(h):
                return black_price(h, K, tau, volatility(h), kind)/h
            greeks = black_greeks(H, K, tau, volatility(H), kind)
            m = K/H
            delta = smile_adjusted_delta(greeks["delta_forward"], greeks["vega"],
                                         -0.15+0.8*(m-1), m, H)
            n = inverse_hedge_normalized(H, delta, greeks["price"], H)
            numeric = H*(q(H+0.1)-q(H-0.1))/0.2
            fd_errors.append(abs(n-numeric))
    assert max(fd_errors) < 1e-8
    m = np.array([0.8, 0.9, 1.0, 1.1, 1.2])
    vol = 0.6-0.2*(m-1)+0.3*(m-1)**2
    fit = local_quadratic_smile(m, vol, vol-0.01, vol+0.02, 1.0)
    assert abs(fit["slope"]+0.2) < 1e-12
    w = np.array(fit["slope_weights"])
    worst = np.where(w >= 0, vol+0.02, vol-0.01)
    assert abs(float(w@worst)-fit["slope_upper"]) < 1e-12
    x = np.linspace(-2, 2, 100)[:, None]
    z = 0.01*np.cos(np.arange(100))
    correction = 0.2+0.3*x[:, 0]
    model = fit_residual_ridge(x, z*correction, z, alpha=0)
    assert np.max(abs(model.predict(x)-correction)) < 1e-10
    saved_mean = model.mean.copy()
    model.predict(np.array([[1000.0]]))
    assert np.array_equal(model.mean, saved_mean)
    return {"price_iv_roundtrips": len(roundtrip_errors),
            "max_iv_error": max(roundtrip_errors), "sm_finite_differences": len(fd_errors),
            "max_sm_fd_error": max(fd_errors), "quote_band_bound_check": True,
            "risk_ridge_recovery_and_normalization_check": True}


if __name__ == "__main__":
    print(json.dumps(self_check(), indent=2))
