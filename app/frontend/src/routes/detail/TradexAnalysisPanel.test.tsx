import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import { TradexAnalysisPanel } from './TradexAnalysisPanel';

const fmtPercent = (value: number | null | undefined, digits = 1) =>
  value == null ? '--' : `${(value * 100).toFixed(digits)}%`;
const fmtSignedPercent = (value: number | null | undefined, digits = 1) =>
  value == null ? '--' : `${(value * 100).toFixed(digits)}%`;
const fmtNumber = (value: number | null | undefined, digits = 2) =>
  value == null ? '--' : value.toFixed(digits);

describe('TradexAnalysisPanel', () => {
  it('renders the read-only analysis summary', () => {
    const markup = renderToStaticMarkup(
      <TradexAnalysisPanel
        state={{
          available: true,
          reason: null,
          loading: false,
          analysis: {
            symbol: '7203',
            asof: '2026-03-19',
            source: 'tradex',
            displayLabel: 'TRADEX',
            sideRatios: { buy: 0.61, neutral: 0.24, sell: 0.15 },
            confidence: 0.77,
            reasons: ['tone=up', 'pattern=breakout', 'ev=positive'],
            candidateComparisons: [
              {
                candidateKey: 'trend_up',
                baselineKey: 'baseline',
                comparisonScope: 'decision_scenarios',
                score: 0.81,
                scoreDelta: 0.12,
                rank: 1,
                reasons: ['trend=up'],
                publishReady: true,
              },
            ],
            publishReadiness: {
              ready: true,
              status: 'ready',
              reasons: ['ready=pass'],
              candidateKey: 'trend_up',
              approved: true,
            },
            overrideState: {
              present: false,
              source: 'none',
              logicKey: 'logic_a',
              logicVersion: 'v7',
              reason: null,
            },
            promotionReview: {
              asOfDate: '2026-03-19',
              championVersion: 'champion-v1',
              challengerVersion: 'challenger-v1',
              sampleCount: 25,
              expectancyDelta: 0.018,
              improvedExpectancy: true,
              maeNonWorse: true,
              adverseMoveNonWorse: true,
              stableWindow: true,
              alignmentOk: true,
              readinessPass: true,
              reasonCodes: ['readiness_pass'],
              approvalDecision: {
                decisionId: 'pub:approved:1',
                decision: 'approved',
                note: 'ready_for_authoritative_cutover',
                actor: 'codex',
                createdAt: '2026-03-19T09:00:00Z',
              },
            },
            forecastSurface: {
              code: '7203',
              asof: '2026-03-19',
              rows: [
                {
                  code: '7203',
                  side: 'long',
                  actionState: 'enter',
                  directionProb: 0.71,
                  expectedUpside: 0.084,
                  expectedDownside: -0.031,
                  invalidationPrice: 1234.5,
                  reasonCodes: ['signal_buy_qualified'],
                  setupTags: ['breakout'],
                  opportunityScore: 0.63,
                  freshnessState: 'fresh',
                },
                {
                  code: '7203',
                  side: 'short',
                  actionState: 'wait',
                  directionProb: 0.29,
                  expectedUpside: 0.021,
                  expectedDownside: -0.054,
                  invalidationPrice: 1222.1,
                  reasonCodes: ['borrow_pressure_supports_short'],
                  setupTags: ['risk_off'],
                  opportunityScore: 0.22,
                  freshnessState: 'fresh',
                },
              ],
            },
          },
        }}
        formatPercentLabel={fmtPercent}
        formatSignedPercentLabel={fmtSignedPercent}
        formatNumber={fmtNumber}
      />
    );

    expect(markup).toContain('published logic / read only');
    expect(markup).toContain('tone');
    expect(markup).toContain('confidence');
    expect(markup).toContain('version');
    expect(markup).toContain('Top 3 reasons');
    expect(markup).toContain('tone=up');
    expect(markup).toContain('Top 3 candidate comparisons');
    expect(markup).toContain('trend_up');
    expect(markup).toContain('v7');
    expect(markup).toContain('77.0%');
    expect(markup).toContain('up');
    expect(markup).toContain('authoritative');
    expect(markup).toContain('decision approved');
    expect(markup).toContain('Forecast surface');
    expect(markup).toContain('enter');
    expect(markup).toContain('expected_upside');
    expect(markup).toContain('signal_buy_qualified');
  });

  it('renders unavailable state with reason', () => {
    const markup = renderToStaticMarkup(
      <TradexAnalysisPanel
        state={{ available: false, reason: 'feature flag disabled', analysis: null, loading: false }}
        formatPercentLabel={fmtPercent}
        formatSignedPercentLabel={fmtSignedPercent}
        formatNumber={fmtNumber}
      />
    );

    expect(markup).toContain('analysis unavailable: feature flag disabled');
  });
});
