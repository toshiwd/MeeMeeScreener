export { };

declare global {
    interface Window {
        pywebview?: {
            api: {
                save_screenshot: (base64: string, filename: string) => Promise<{ success: boolean; savedDir?: string; error?: string }>;
                open_path: (path: string) => Promise<void>;
                export_perf_diagnostics?: (payload?: unknown) => Promise<{
                    success: boolean;
                    directory?: string;
                    writtenFiles?: string[];
                    existingFiles?: string[];
                    error?: string;
                }>;
                clear_perf_diagnostics?: () => Promise<{ success: boolean; removedCount?: number; error?: string }>;
                open_perf_diagnostics_dir?: () => Promise<boolean>;
            };
        };
        MeeMeePerfDiagnostics?: {
            enabled: () => boolean;
            export: () => Promise<unknown>;
            clear: () => Promise<unknown>;
            openDir: () => Promise<unknown>;
            getEvents: () => unknown[];
            enable: () => void;
            disable: () => void;
        };
    }
}
