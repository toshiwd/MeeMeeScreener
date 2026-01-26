export { };

declare global {
    interface Window {
        pywebview?: {
            api: {
                save_screenshot: (base64: string, filename: string) => Promise<{ success: boolean; savedDir?: string; error?: string }>;
                open_path: (path: string) => Promise<void>;
            };
        };
    }
}
