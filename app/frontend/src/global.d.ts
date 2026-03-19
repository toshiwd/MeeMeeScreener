/// <reference types="vite/client" />

declare const __APP_VERSION__: string;

interface ImportMetaEnv {
  readonly DEV: boolean;
  readonly VITE_GRID_REFACTOR?: string;
  readonly VITE_SHOW_OPERATOR_CONSOLE?: string;
  readonly VITE_ENABLE_TRADEX_DETAIL_ANALYSIS?: string;
  readonly VITE_ENABLE_TRADEX_LIST_SUMMARY?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
