/// <reference types="vite/client" />

declare const __APP_VERSION__: string;

interface ImportMetaEnv {
  readonly DEV: boolean;
  readonly VITE_GRID_REFACTOR?: string;
  readonly VITE_SHOW_OPERATOR_CONSOLE?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
