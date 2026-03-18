/**
 * Window screenshot utility - captures visible viewport as PNG
 * Uses canvas-based approach for better compatibility with chart libraries
 */

type ScreenshotOptions = {
    screenType: string;
    code?: string | null;
};

type CaptureResult = {
    success: boolean;
    blob?: Blob;
    filename?: string;
    error?: string;
};

type CopyResult = {
    success: boolean;
    copied: boolean;
    blob?: Blob;
    filename?: string;
    error?: string;
};

const buildFilename = (screenType: string, code?: string | null): string => {
    const now = new Date();
    const yyyy = String(now.getFullYear());
    const mm = String(now.getMonth() + 1).padStart(2, "0");
    const dd = String(now.getDate()).padStart(2, "0");
    const hh = String(now.getHours()).padStart(2, "0");
    const min = String(now.getMinutes()).padStart(2, "0");
    const ss = String(now.getSeconds()).padStart(2, "0");
    const timestamp = `${yyyy}${mm}${dd}_${hh}${min}${ss}`;
    const safeCode = code ? code.replace(/[^a-zA-Z0-9]/g, "_") : "none";
    return `MeeMee_${screenType}_${safeCode}_${timestamp}.png`;
};

const waitForRender = (): Promise<void> => {
    return new Promise((resolve) => {
        requestAnimationFrame(() => {
            requestAnimationFrame(() => {
                setTimeout(resolve, 100);
            });
        });
    });
};

const captureCanvasElements = (root: HTMLElement): Map<HTMLCanvasElement, string> => {
    const canvasMap = new Map<HTMLCanvasElement, string>();
    const canvases = root.querySelectorAll("canvas");
    canvases.forEach((canvas) => {
        try {
            const dataUrl = canvas.toDataURL("image/png");
            canvasMap.set(canvas, dataUrl);
        } catch {
            // Cross-origin canvas, skip
        }
    });
    return canvasMap;
};

const resolveCaptureBackground = (root: HTMLElement): string | null => {
    const candidates = [document.body, document.documentElement, root].filter(
        (node): node is HTMLElement => Boolean(node)
    );
    for (const node of candidates) {
        const style = getComputedStyle(node);
        const backgroundImage = style.backgroundImage;
        if (backgroundImage && backgroundImage !== "none") {
            return null;
        }
        const backgroundColor = style.backgroundColor;
        if (
            backgroundColor &&
            backgroundColor !== "transparent" &&
            backgroundColor !== "rgba(0, 0, 0, 0)"
        ) {
            return backgroundColor;
        }
    }
    const fallback = getComputedStyle(document.documentElement)
        .getPropertyValue("--theme-bg-primary")
        .trim();
    return fallback || null;
};

/**
 * Capture the visible window as a PNG Blob
 */
export const captureWindowBlob = async (
    options: ScreenshotOptions
): Promise<CaptureResult> => {
    try {
        await waitForRender();

        const root = document.getElementById("root");
        if (!root) {
            return { success: false, error: "ルート要素が見つかりません" };
        }

        const captureRoot = document.body ?? root;
        const backgroundColor = resolveCaptureBackground(root);

        // Pre-capture canvas elements before cloning (lightweight-charts uses canvas)
        captureCanvasElements(root);

        // Dynamically import html2canvas
        let html2canvas: (element: HTMLElement, options?: object) => Promise<HTMLCanvasElement>;
        try {
            const module = await import("html2canvas");
            html2canvas = module.default;
        } catch {
            return { success: false, error: "html2canvasの読み込みに失敗しました" };
        }

        // Capture with html2canvas
        const canvas = await html2canvas(captureRoot, {
            useCORS: true,
            allowTaint: true,
            scale: window.devicePixelRatio || 1,
            logging: false,
            backgroundColor,
            windowWidth: captureRoot.scrollWidth,
            windowHeight: captureRoot.scrollHeight,
            width: window.innerWidth,
            height: window.innerHeight,
            x: 0,
            y: 0,
        });

        // Convert to blob
        const blob = await new Promise<Blob | null>((resolve) => {
            canvas.toBlob((b) => resolve(b), "image/png");
        });

        if (!blob) {
            return { success: false, error: "画像の生成に失敗しました" };
        }

        const filename = buildFilename(options.screenType, options.code);
        return { success: true, blob, filename };
    } catch (error) {
        const message = error instanceof Error ? error.message : "スクリーンショットに失敗しました";
        return { success: false, error: message };
    }
};

/**
 * Copy a Blob image to clipboard
 */
export const copyBlobToClipboard = async (blob: Blob): Promise<boolean> => {
    try {
        // Check for File System Access API
        if (!navigator.clipboard || !("write" in navigator.clipboard)) {
            return false;
        }

        // Check if ClipboardItem is available
        if (typeof ClipboardItem === "undefined") {
            return false;
        }

        const item = new ClipboardItem({ "image/png": blob });
        await navigator.clipboard.write([item]);
        return true;
    } catch {
        // Permission denied, not secure context, or other error
        return false;
    }
};

// Define pywebview interface
declare global {
    interface Window {
        pywebview?: {
            api: {
                save_screenshot: (dataUri: string, filename: string) => Promise<{
                    success: boolean;
                    savedPath?: string;
                    savedDir?: string;
                    fileName?: string;
                    error?: string;
                }>;
                open_path: (path: string) => Promise<boolean>;
                open_screenshot_dir: () => Promise<boolean>;
            };
        };
    }
}

/**
 * Save a Blob to file via backend (preferred) or download
 */
export const saveBlobToFile = async (blob: Blob, filename: string): Promise<{
    success: boolean;
    savedPath?: string;
    savedDir?: string;
    fileName?: string;
    error?: string;
}> => {
    // 1. Try pywebview backend first
    if (window.pywebview) {
        try {
            const dataUri = await new Promise<string>((resolve) => {
                const reader = new FileReader();
                reader.onloadend = () => resolve(reader.result as string);
                reader.readAsDataURL(blob);
            });
            return await window.pywebview.api.save_screenshot(dataUri, filename);
        } catch (e) {
            console.error("Backend save failed:", e);
            // Fallthrough to download
        }
    }

    // 2. Fallback: trigger download
    try {
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = filename;
        link.style.display = "none";
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
        return { success: true, fileName: filename };
    } catch {
        return { success: false, error: "保存に失敗しました" };
    }
};

/**
 * Main screenshot function: capture → copy to clipboard (save as fallback)
 * Returns result with blob for potential later save action
 */
export const captureAndCopyScreenshot = async (
    options: ScreenshotOptions
): Promise<CopyResult> => {
    const captureResult = await captureWindowBlob(options);

    if (!captureResult.success || !captureResult.blob) {
        return {
            success: false,
            copied: false,
            error: captureResult.error,
        };
    }

    const blob = captureResult.blob;
    const filename = captureResult.filename!;

    // Try to copy to clipboard first
    const copied = await copyBlobToClipboard(blob);

    return {
        success: true,
        copied,
        blob,
        filename,
    };
};

export const getScreenType = (pathname: string): string => {
    if (pathname.startsWith("/practice/")) return "Practice";
    if (pathname.startsWith("/detail/")) return "Detail";
    if (pathname === "/ranking") return "Ranking";
    if (pathname === "/favorites") return "Favorites";
    if (pathname === "/candidates") return "Candidates";
    if (pathname === "/tradex-tags") return "AIResearch";
    if (pathname === "/") return "Grid";
    return "Screen";
};

// Legacy function for backward compatibility
export const captureWindowScreenshot = async (
    options: ScreenshotOptions
): Promise<{ success: boolean; filename?: string; error?: string }> => {
    const result = await captureAndCopyScreenshot(options);
    if (!result.success) {
        return { success: false, error: result.error };
    }
    if (result.copied) {
        return { success: true, filename: result.filename };
    }
    // Fallback to save
    if (result.blob && result.filename) {
        const saveResult = await saveBlobToFile(result.blob, result.filename);
        if (saveResult.success) {
            return { success: true, filename: result.filename };
        }
    }
    return { success: false, error: "保存に失敗しました" };
};
