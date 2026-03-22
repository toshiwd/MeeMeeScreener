/**
 * Theme management utility
 * Handles dark/light mode switching with localStorage persistence
 */

export type Theme = "dark" | "light";

const STORAGE_KEY = "meemee-theme";

export const getStoredTheme = (): Theme => {
    if (typeof window === "undefined") return "light";
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (stored === "light" || stored === "dark") return stored;
    return "light";
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
    if (typeof document === "undefined") return "light";
    const value = document.documentElement.getAttribute("data-theme");
    return value === "dark" ? "dark" : "light";
};

export const toggleTheme = (current: Theme): Theme => {
    return current === "dark" ? "light" : "dark";
};
