import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import { DetailAnalysisPanel } from './DetailAnalysisPanel';

const fmtPercent = (value: number | null | undefined, digits = 1) =>
  value == null ? '--' : `${(value * 100).toFixed(digits)}%`;
const fmtSignedPercent = (value: number | null | undefined, digits = 1) =>
  value == null ? '--' : `${(value * 100).toFixed(digits)}%`;
const fmtNumber = (value: number | null | undefined, digits = 2) =>
  value == null ? '--' : value.toFixed(digits);

describe('DetailAnalysisPanel', () => {
  it('keeps the analysis panel read-only and uses provisional labels', () => {
    const markup = renderToStaticMarkup(
      <DetailAnalysisPanel
        analysisAsOfTime={1773878400}
        analysisBackfillActive={false}
        analysisRecalcSubmitting={null}
        analysisRecalcDisabled={false}
        analysisRecalcDisabledReason={null}
        submitAnalysisRecalc={async () => {}}
        analysisDtLabel="26/03/19"
        cursorMode={true}
        analysisCursorDateLabel="26-03-19"
        canShowPhase={true}
        phaseReasons={['phase=trend']}
        canShowAnalysis={true}
        analysisDecision={{
          tone: 'up',
          sideLabel: '買い',
          patternLabel: 'breakout',
          environmentLabel: 'strong',
          confidence: 0.72,
          buyProb: 0.64,
          sellProb: 0.21,
          neutralProb: 0.15,
          version: 'v1',
          scenarios: [],
        }}
        analysisSummaryLoading={true}
        analysisGuidance={{
          confidenceRank: 'A',
          action: 'watch',
          watchpoint: '暫定',
          buyWidth: 64,
          sellWidth: 21,
          neutralWidth: 15,
          buySetupProb: 0.51,
          sellSetupProb: 0.27,
          buySetupWidth: 51,
          sellSetupWidth: 27,
          buySetupState: '実行',
          sellSetupState: '監視',
        }}
        analysisEntryPolicy={{
          up: { setupType: 'breakout', recommendedHoldDays: 5, recommendedHoldReason: 'x' },
          down: { setupType: 'breakdown', recommendedHoldDays: 3, recommendedHoldReason: 'y' },
        }}
        patternSummary={{ environmentLabel: 'strong' }}
        analysisPreparationVisible={false}
        analysisBackfillProgressLabel={null}
        analysisBackfillMessage={null}
        sellAnalysisDtLabel="26/03/18"
        sellPredDtLabel="26/03/20"
        researchPriorRunId="run-1"
        analysisResearchPrior={{} as never}
        researchPriorUpMeta="up-meta"
        researchPriorDownMeta="down-meta"
        edinetStatusMeta="EDINET ok"
        edinetQualityMeta="good"
        edinetMetricsMeta="metrics"
        edinetBonusMeta="bonus"
        hasSwingData={false}
        swingPlan={null}
        swingSideLabel="--"
        swingReasonsLabel=""
        swingDiagnostics={null}
        swingSetupExpectancy={null}
        analysisMissingDataVisible={true}
        formatPercentLabel={fmtPercent}
        formatNumber={fmtNumber}
        formatSignedPercentLabel={fmtSignedPercent}
      />
    );

    expect(markup).toContain('判定確認');
    expect(markup).toContain('暫定');
    expect(markup).toContain('基準日 26/03/19');
    expect(markup).toContain('カーソル日 26-03-19');
    expect(markup).toContain('売買サマリー');
    expect(markup).toContain('買い候補');
    expect(markup).toContain('売り候補');
    expect(markup).toContain('基準日を中心に130本を再計算');
    expect(markup).not.toContain('読込中...');
  });
});
