import { TradexLegacyFrame } from "../LegacyViewFrame";
import TradexTagValidationView from "../../routes/TradexTagValidationView";
import PublishOpsView from "../../routes/PublishOpsView";
import ToredexSimulationView from "../../routes/ToredexSimulationView";

export function TradexLegacyTagsPage() {
  return (
    <TradexLegacyFrame title="検証（旧）" description="移行中の旧検証画面です。まずは新しい候補比較と反映判定を使ってください。">
      <div className="tradex-legacy-embed">
        <TradexTagValidationView />
      </div>
    </TradexLegacyFrame>
  );
}

export function TradexLegacyPublishPage() {
  return (
    <TradexLegacyFrame title="反映（旧）" description="移行中の旧反映画面です。新しい反映判定を優先してください。">
      <div className="tradex-legacy-embed">
        <PublishOpsView />
      </div>
    </TradexLegacyFrame>
  );
}

export function TradexLegacySimPage() {
  return (
    <TradexLegacyFrame title="検証シミュレーション（旧）" description="移行中の旧シミュレーション画面です。">
      <div className="tradex-legacy-embed">
        <ToredexSimulationView />
      </div>
    </TradexLegacyFrame>
  );
}
