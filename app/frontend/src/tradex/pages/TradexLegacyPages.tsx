import LegacyViewFrame from "../LegacyViewFrame";
import PublishOpsRouteGuard from "../../routes/PublishOpsRouteGuard";
import PublishOpsView from "../../routes/PublishOpsView";
import ToredexSimulationView from "../../routes/ToredexSimulationView";
import TradexTagValidationView from "../../routes/TradexTagValidationView";

export function TradexLegacyTagsPage() {
  return (
    <LegacyViewFrame title="検証（旧）" description="移行中の検証画面です。新しい TRADEX では候補比較と反映判定を優先します。">
      <div className="tradex-legacy-embed">
        <TradexTagValidationView />
      </div>
    </LegacyViewFrame>
  );
}

export function TradexLegacyPublishPage() {
  return (
    <LegacyViewFrame title="反映（旧）" description="移行中の運用画面です。通常導線ではなく legacy 配下に閉じ込めています。">
      <div className="tradex-legacy-embed">
        <PublishOpsRouteGuard>
          <PublishOpsView />
        </PublishOpsRouteGuard>
      </div>
    </LegacyViewFrame>
  );
}

export function TradexLegacySimPage() {
  return (
    <LegacyViewFrame title="検証シミュレーション（旧）" description="候補確認の補助に使う移行中画面です。">
      <div className="tradex-legacy-embed">
        <ToredexSimulationView />
      </div>
    </LegacyViewFrame>
  );
}
