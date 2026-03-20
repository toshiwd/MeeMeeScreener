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
    expect(markup).toContain('\u8cb7\u3044\u5bc4\u308a');
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
