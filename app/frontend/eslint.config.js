import js from "@eslint/js";
import globals from "globals";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["dist", "node_modules"] },
  {
    files: ["**/*.{ts,tsx}"],
    extends: [js.configs.recommended, ...tseslint.configs.recommended],
    languageOptions: {
      ecmaVersion: 2020,
      sourceType: "module",
      globals: globals.browser,
    },
    plugins: {
      "react-hooks": reactHooks,
      "react-refresh": reactRefresh,
    },
    rules: {
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "warn",
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-unused-vars": [
        "warn",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          ignoreRestSiblings: true,
        },
      ],
      "prefer-const": "warn",
      "react-refresh/only-export-components": [
        "warn",
        { allowConstantExport: true },
      ],
    },
  },
  {
    files: [
      "e2e/ops-publish.real-backend.smoke.spec.ts",
      "src/components/DetailChart.tsx",
      "src/components/PositionOverlay.tsx",
      "src/components/Sparkline.tsx",
      "src/components/StockTile.tsx",
      "src/components/layout/Header.tsx",
      "src/routes/DetailView.tsx",
      "src/routes/FavoritesView.tsx",
      "src/routes/GridView.tsx",
      "src/routes/MarketView.tsx",
      "src/routes/RankingView.tsx",
      "src/routes/ToredexSimulationView.tsx",
      "src/routes/detail/DetailAnalysisPanel.tsx",
      "src/routes/detail/detailHelpers.ts",
      "src/routes/detail/hooks/useExactDecisionRange.ts",
      "src/routes/grid/gridHelpers.ts",
      "src/utils/consultation.ts",
      "src/utils/drawingInteraction.ts",
      "src/utils/signals.ts",
      "src/utils/technicalFilter.ts"
    ],
    rules: {
      "@typescript-eslint/ban-ts-comment": "off"
    }
  }
);
