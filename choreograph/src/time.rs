//! Time as integer milliseconds.
//!
//! The library never reads a clock: the caller passes [`Time`] in. Integers keep plans and traces bit-exact on
//! every machine. The epoch is the caller's choice (Unix time in a real executor, 0 in a simulation).

use std::fmt;
use std::ops::{Add, AddAssign, Sub};
use std::str::FromStr;

/// A point in time, milliseconds since the caller's epoch.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct Time(pub i64);

/// A length of time in milliseconds. May be negative (a slack that has run out).
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct Dur(pub i64);

impl Time {
    /// The epoch.
    pub const ZERO: Time = Time(0);
    /// Later than any real time; "never".
    pub const MAX: Time = Time(i64::MAX);

    /// Milliseconds since the epoch.
    pub const fn ms(self) -> i64 {
        self.0
    }
}

impl Dur {
    pub const ZERO: Dur = Dur(0);

    pub const fn ms(ms: i64) -> Dur {
        Dur(ms)
    }
    pub const fn secs(s: i64) -> Dur {
        Dur(s * 1000)
    }
    pub const fn mins(m: i64) -> Dur {
        Dur(m * 60_000)
    }
    pub const fn hours(h: i64) -> Dur {
        Dur(h * 3_600_000)
    }
    /// Milliseconds.
    pub const fn as_ms(self) -> i64 {
        self.0
    }
    /// Seconds as a float, for formulas and display.
    pub fn as_secs_f64(self) -> f64 {
        self.0 as f64 / 1000.0
    }
}

impl Add<Dur> for Time {
    type Output = Time;
    fn add(self, d: Dur) -> Time {
        Time(self.0.saturating_add(d.0))
    }
}

impl AddAssign<Dur> for Time {
    fn add_assign(&mut self, d: Dur) {
        *self = *self + d;
    }
}

impl Sub<Dur> for Time {
    type Output = Time;
    fn sub(self, d: Dur) -> Time {
        Time(self.0.saturating_sub(d.0))
    }
}

impl Sub for Time {
    type Output = Dur;
    fn sub(self, o: Time) -> Dur {
        Dur(self.0.saturating_sub(o.0))
    }
}

impl Add for Dur {
    type Output = Dur;
    fn add(self, o: Dur) -> Dur {
        Dur(self.0.saturating_add(o.0))
    }
}

impl AddAssign for Dur {
    fn add_assign(&mut self, o: Dur) {
        *self = *self + o;
    }
}

impl Sub for Dur {
    type Output = Dur;
    fn sub(self, o: Dur) -> Dur {
        Dur(self.0.saturating_sub(o.0))
    }
}

impl std::iter::Sum for Dur {
    fn sum<I: Iterator<Item = Dur>>(iter: I) -> Dur {
        iter.fold(Dur::ZERO, |a, b| a + b)
    }
}

/// Error from parsing a duration such as `1h30m`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("not a duration: {0:?} (examples: 250ms, 90s, 5m, 1h30m, 2d)")]
pub struct ParseDurError(pub String);

const UNITS: [(&str, i64); 5] = [
    ("d", 86_400_000),
    ("h", 3_600_000),
    ("m", 60_000),
    ("s", 1000),
    ("ms", 1),
];

impl FromStr for Dur {
    type Err = ParseDurError;

    /// Accepts a sequence of `<number><unit>` with units `d`, `h`, `m`, `s`, `ms` (`1h30m`, `2.5s`, `-5m`), or
    /// a bare integer of milliseconds.
    fn from_str(s: &str) -> Result<Dur, ParseDurError> {
        let err = || ParseDurError(s.to_string());
        let t = s.trim();
        let (neg, body) = match t.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, t),
        };
        if body.is_empty() {
            return Err(err());
        }
        if let Ok(ms) = body.parse::<i64>() {
            return Ok(Dur(if neg { -ms } else { ms }));
        }
        let mut total: f64 = 0.0;
        let mut rest = body;
        while !rest.is_empty() {
            let num_len = rest
                .find(|c: char| !(c.is_ascii_digit() || c == '.'))
                .ok_or_else(err)?;
            if num_len == 0 {
                return Err(err());
            }
            let num: f64 = rest[..num_len].parse().map_err(|_| err())?;
            rest = &rest[num_len..];
            let unit_len = rest
                .find(|c: char| c.is_ascii_digit() || c == '.')
                .unwrap_or(rest.len());
            let unit = &rest[..unit_len];
            let scale = UNITS
                .iter()
                .find(|(u, _)| *u == unit)
                .map(|(_, f)| *f)
                .ok_or_else(err)?;
            total += num * scale as f64;
            rest = &rest[unit_len..];
        }
        let ms = total.round() as i64;
        Ok(Dur(if neg { -ms } else { ms }))
    }
}

impl fmt::Display for Dur {
    /// The shortest exact form: `1h30m`, `90s` prints as `1m30s`, `250ms`, `0s`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 0 {
            return f.write_str("0s");
        }
        if self.0 < 0 {
            f.write_str("-")?;
        }
        let mut left = self.0.unsigned_abs();
        for (unit, scale) in UNITS {
            let scale = scale as u64;
            if left >= scale {
                write!(f, "{}{unit}", left / scale)?;
                left %= scale;
            }
        }
        Ok(())
    }
}

impl fmt::Display for Time {
    /// Time since the epoch, as a duration (`t+1h30m`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t+{}", Dur(self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_print() {
        for (s, ms, back) in [
            ("1h30m", 5_400_000, "1h30m"),
            ("90s", 90_000, "1m30s"),
            ("250ms", 250, "250ms"),
            ("2.5s", 2500, "2s500ms"),
            ("2d", 172_800_000, "2d"),
            ("-5m", -300_000, "-5m"),
            ("1500", 1500, "1s500ms"),
            ("0s", 0, "0s"),
        ] {
            let d: Dur = s.parse().unwrap();
            assert_eq!(d, Dur(ms), "{s}");
            assert_eq!(d.to_string(), back, "{s}");
        }
    }

    #[test]
    fn parse_errors() {
        for s in ["", "h", "5x", "1h-2m", "-", "m5"] {
            assert!(s.parse::<Dur>().is_err(), "{s:?} should not parse");
        }
    }

    #[test]
    fn arithmetic() {
        let t = Time(1000) + Dur::secs(2);
        assert_eq!(t, Time(3000));
        assert_eq!(t - Time(500), Dur(2500));
        assert_eq!(Time::MAX + Dur::secs(1), Time::MAX);
        assert_eq!(t.to_string(), "t+3s");
    }
}
