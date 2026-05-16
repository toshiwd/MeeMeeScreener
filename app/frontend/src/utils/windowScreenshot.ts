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

const SCREENSHOT_CANVAS_ATTR = "data-meemee-screenshot-canvas-id";

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

const shouldForceScreenshotFailure = (): boolean => {
    const globalWindow = window as Window & {
        __meemeeFatalDiagnosticsForceScreenshotFailure?: boolean;
    };
    if (Boolean(globalWindow.__meemeeFatalDiagnosticsForceScreenshotFailure)) {
        return true;
    }
    try {
        const probe = new URLSearchParams(window.location.search).get("meemeeFatalDiagnostics");
        return probe === "window-error-no-root";
    } catch {
        return false;
    }
};

const isSafeHtml2CanvasColor = (value: string | null | undefined): value is string => {
    if (!value) {
        return false;
    }
    const normalized = value.trim();
    if (!normalized) {
        return false;
    }
    const lower = normalized.toLowerCase();
    if (
        lower.includes("color-mix(") ||
        lower.includes("color(") ||
        lower.includes("lab(") ||
        lower.includes("lch(") ||
        lower.includes("oklab(") ||
        lower.includes("oklch(") ||
        lower.includes("var(")
    ) {
        return false;
    }
    return /^#|^rgba?\(|^hsla?\(|^[a-z]+$/.test(lower);
};

const readSafeThemeColor = (name: string): string | null => {
    const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return isSafeHtml2CanvasColor(value) ? value : null;
};

const captureCanvasElements = (root: HTMLElement): {
    snapshots: Map<string, string>;
    cleanup: () => void;
} => {
    const snapshots = new Map<string, string>();
    const previousAttributes: Array<{ canvas: HTMLCanvasElement; value: string | null }> = [];
    const canvases = root.querySelectorAll("canvas");
    const stamp = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    canvases.forEach((canvas, index) => {
        try {
            const dataUrl = canvas.toDataURL("image/png");
            const existing = canvas.getAttribute(SCREENSHOT_CANVAS_ATTR);
            const id = existing || `canvas-${stamp}-${index}`;
            previousAttributes.push({ canvas, value: existing });
            if (!existing) {
                canvas.setAttribute(SCREENSHOT_CANVAS_ATTR, id);
            }
            snapshots.set(id, dataUrl);
        } catch {
            // Cross-origin canvas, skip
        }
    });
    return {
        snapshots,
        cleanup: () => {
            previousAttributes.forEach(({ canvas, value }) => {
                if (value == null) {
                    canvas.removeAttribute(SCREENSHOT_CANVAS_ATTR);
                } else {
                    canvas.setAttribute(SCREENSHOT_CANVAS_ATTR, value);
                }
            });
        }
    };
};

const applyCanvasSnapshotsToClone = (clonedDocument: Document, snapshots: Map<string, string>) => {
    snapshots.forEach((dataUrl, id) => {
        const clonedCanvas = Array.from(
            clonedDocument.querySelectorAll<HTMLCanvasElement>(`canvas[${SCREENSHOT_CANVAS_ATTR}]`)
        ).find((canvas) => canvas.getAttribute(SCREENSHOT_CANVAS_ATTR) === id) ?? null;
        if (!clonedCanvas) return;
        const image = clonedDocument.createElement("img");
        image.src = dataUrl;
        image.width = clonedCanvas.width;
        image.height = clonedCanvas.height;
        image.className = clonedCanvas.className;
        image.setAttribute("aria-hidden", "true");
        image.style.cssText = clonedCanvas.style.cssText;
        clonedCanvas.replaceWith(image);
    });
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
            backgroundColor !== "rgba(0, 0, 0, 0)" &&
            isSafeHtml2CanvasColor(backgroundColor)
        ) {
            return backgroundColor;
        }
    }
    const fallback = readSafeThemeColor("--bg-app") ?? readSafeThemeColor("--theme-bg-primary");
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

        if (shouldForceScreenshotFailure()) {
            return { success: false, error: "スクリーンショットに失敗しました" };
        }

        const root = document.getElementById("root");
        if (!root) {
            return { success: false, error: "ルート要素が見つかりません" };
        }

        const captureRoot = document.body ?? root;
        const backgroundColor = resolveCaptureBackground(root);

        // Dynamically import html2canvas
        let html2canvas: (element: HTMLElement, options?: object) => Promise<HTMLCanvasElement>;
        try {
            const module = await import("html2canvas");
            html2canvas = module.default;
        } catch {
            return { success: false, error: "スクリーンショット機能の読み込みに失敗しました" };
        }

        const canvasSnapshots = captureCanvasElements(root);
        let canvas: HTMLCanvasElement;
        try {
            canvas = await html2canvas(captureRoot, {
                useCORS: true,
                allowTaint: true,
                foreignObjectRendering: true,
                scale: window.devicePixelRatio || 1,
                logging: false,
                backgroundColor,
                windowWidth: captureRoot.scrollWidth,
                windowHeight: captureRoot.scrollHeight,
                width: window.innerWidth,
                height: window.innerHeight,
                x: 0,
                y: 0,
                onclone: (clonedDocument: Document) => {
                    applyCanvasSnapshotsToClone(clonedDocument, canvasSnapshots.snapshots);
                },
            });
        } finally {
            canvasSnapshots.cleanup();
        }

        // Convert to blob
        const blob = await new Promise<Blob | null>((resolve) => {
            canvas.toBlob((b) => resolve(b), "image/png");
        });

        if (!blob) {
            return { success: false, error: "画像の生成に失敗しました" };
        }

        const filename = buildFilename(options.screenType, options.code);
        return { success: true, blob, filename };
    } catch {
        return { success: false, error: "スクリーンショットに失敗しました" };
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
    } catch (error) {
        console.error("Clipboard screenshot write failed:", error);
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
                save_perf_diagnostic_artifact: (dataUri: string, filename: string) => Promise<{
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

const blobToDataUri = async (blob: Blob): Promise<string> => {
    return await new Promise<string>((resolve) => {
        const reader = new FileReader();
        reader.onloadend = () => resolve(reader.result as string);
        reader.readAsDataURL(blob);
    });
};

const saveBlobViaPywebview = async (
    blob: Blob,
    filename: string,
    method: "save_screenshot" | "save_perf_diagnostic_artifact"
): Promise<{
    success: boolean;
    savedPath?: string;
    savedDir?: string;
    fileName?: string;
    error?: string;
} | null> => {
    if (!window.pywebview) {
        return null;
    }
    try {
        const dataUri = await blobToDataUri(blob);
        const api = window.pywebview.api;
        const saver = api?.[method];
        if (!saver) {
            return null;
        }
        return await saver(dataUri, filename);
    } catch (e) {
        console.error("Backend save failed:", e);
        return null;
    }
};

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
    const backendResult = await saveBlobViaPywebview(blob, filename, "save_screenshot");
    if (backendResult) {
        return backendResult;
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
 * Save a Blob into the diagnostics directory via backend (preferred) or download fallback
 */
export const saveBlobToPerfDiagnostics = async (blob: Blob, filename: string): Promise<{
    success: boolean;
    savedPath?: string;
    savedDir?: string;
    fileName?: string;
    error?: string;
}> => {
    const backendResult = await saveBlobViaPywebview(blob, filename, "save_perf_diagnostic_artifact");
    if (backendResult) {
        return backendResult;
    }

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
    if (pathname === "/tradex/verify" || pathname === "/tradex/legacy/tags") return "TRADEX";
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
