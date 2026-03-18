/// <reference types="vite/client" />

declare const __APP_VERSION__: string;

interface ImportMetaEnv {
  readonly DEV: boolean;
  readonly VITE_GRID_REFACTOR?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
