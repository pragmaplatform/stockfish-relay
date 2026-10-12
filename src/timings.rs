use std::{collections::VecDeque, time::Duration};

use uuid::Uuid;

use crate::protocol::{JobTimings, TimingMetric, TimingSummary};

const WINDOW_SIZE: usize = 1000;

#[derive(Default)]
pub(crate) struct RoundTrips(VecDeque<f64>);

impl RoundTrips {
    pub fn record(&mut self, milliseconds: f64) {
        if self.0.len() == WINDOW_SIZE {
            self.0.pop_front();
        }
        self.0.push_back(milliseconds);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn summary(&self) -> Option<TimingMetric> {
        metric(self.0.iter().copied().collect())
    }
}

pub(crate) struct Sample {
    pub worker_id: Uuid,
    pub retried: bool,
    pub queue_wait: Duration,
    pub engine: Duration,
    pub overhead: Duration,
    pub total: Duration,
}

#[derive(Default)]
pub(crate) struct Timings(VecDeque<Sample>);

impl Timings {
    pub fn record(&mut self, sample: Sample) {
        if self.0.len() == WINDOW_SIZE {
            self.0.pop_front();
        }
        self.0.push_back(sample);
    }

    pub fn summary(&self, worker_id: Option<Uuid>) -> JobTimings {
        let samples: Vec<_> = self
            .0
            .iter()
            .filter(|sample| worker_id.is_none_or(|id| sample.worker_id == id))
            .collect();
        JobTimings {
            first_attempt: summarize(samples.iter().copied().filter(|sample| !sample.retried).collect()),
            retried: summarize(samples.iter().copied().filter(|sample| sample.retried).collect()),
        }
    }
}

fn summarize(samples: Vec<&Sample>) -> TimingSummary {
    let metric = |get: fn(&Sample) -> Duration| -> Option<TimingMetric> {
        metric(
            samples
                .iter()
                .map(|sample| get(sample).as_secs_f64() * 1000.0)
                .collect(),
        )
    };
    TimingSummary {
        samples: samples.len(),
        queue_wait: metric(|sample| sample.queue_wait),
        engine: metric(|sample| sample.engine),
        overhead: metric(|sample| sample.overhead),
        total: metric(|sample| sample.total),
    }
}

fn metric(mut values: Vec<f64>) -> Option<TimingMetric> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    Some(TimingMetric {
        average_ms: values.iter().map(|value| value / values.len() as f64).sum(),
        p95_ms: values[(values.len() * 95).div_ceil(100) - 1],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rolling_window_evicts_old_samples_and_keeps_worker_and_retry_groups_separate() {
        let mut timings = Timings::default();
        let old_worker = Uuid::new_v4();
        let worker = Uuid::new_v4();
        let sample = |worker_id, retried, milliseconds| Sample {
            worker_id,
            retried,
            queue_wait: Duration::from_millis(milliseconds),
            engine: Duration::from_millis(milliseconds),
            overhead: Duration::ZERO,
            total: Duration::from_millis(milliseconds * 2),
        };
        assert!(timings.summary(None).first_attempt.total.is_none());
        timings.record(sample(old_worker, true, 10_000));
        assert_eq!(timings.summary(Some(old_worker)).retried.samples, 1);
        for milliseconds in 1..=1000 {
            timings.record(sample(worker, false, milliseconds));
        }
        let summary = timings.summary(None);
        assert_eq!(summary.first_attempt.samples, 1000);
        assert_eq!(summary.retried.samples, 0);
        let engine = summary.first_attempt.engine.unwrap();
        assert!((engine.average_ms - 500.5).abs() < 1e-9);
        assert_eq!(engine.p95_ms, 950.0);
        assert_eq!(timings.summary(Some(old_worker)).retried.samples, 0);
        assert_eq!(timings.summary(Some(worker)).first_attempt.samples, 1000);
    }

    #[test]
    fn heartbeat_window_preserves_fractional_milliseconds_and_evicts_old_samples() {
        let mut trips = RoundTrips::default();
        assert!(trips.summary().is_none());
        trips.record(10_000.0);
        for _ in 0..1000 {
            trips.record(0.25);
        }
        assert_eq!(trips.len(), 1000);
        let summary = trips.summary().unwrap();
        assert!((summary.average_ms - 0.25).abs() < 1e-9);
        assert_eq!(summary.p95_ms, 0.25);
    }
}
