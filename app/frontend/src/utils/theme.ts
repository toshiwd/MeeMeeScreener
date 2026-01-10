/**
 * Theme management utility
 * Handles dark/light mode switching with localStorage persistence
 */

export type Theme = "dark" | "light";

const STORAGE_KEY = "meemee-theme";

export const getStoredTheme = (): Theme => {
    if (typeof window === "undefined") return "dark";
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (stored === "light" || stored === "dark") return stored;
    // Check system preference
    if (window.matchMedia?.("(prefers-color-scheme: light)").matches) {
        return "light";
    }
    return "dark";
};

export const setStoredTheme = (theme: Theme): void => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(STORAGE_KEY, theme);
};

export const applyTheme = (theme: Theme): void => {
    const root = document.documentElement;
    root.setAttribute("data-theme", theme);
};

export const getDomTheme = (): Theme => {
    if (typeof document === "undefined") return "dark";
    const value = document.documentElement.getAttribute("data-theme");
    return value === "light" ? "light" : "dark";
};

export const toggleTheme = (current: Theme): Theme => {
    return current === "dark" ? "light" : "dark";
};
