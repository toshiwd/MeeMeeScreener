export const getDesktopUiScale = (_devicePixelRatio: number) => 1;

export const getDesktopLayoutViewportHeight = (innerHeight: number, scale: number) => {
  if (!Number.isFinite(innerHeight) || innerHeight <= 0) return 0;
  if (!Number.isFinite(scale) || scale <= 0) return innerHeight;
  return innerHeight / scale;
};

export const installDesktopUiScaleCompensation = () => {
  let frame: number | null = null;

  const applyScale = () => {
    frame = null;
    if (!window.pywebview) return;
    const scale = getDesktopUiScale(window.devicePixelRatio || 1);
    const layoutViewportHeight = getDesktopLayoutViewportHeight(window.innerHeight, scale);
    document.documentElement.style.zoom = String(scale);
    document.documentElement.style.setProperty("--desktop-layout-viewport-height", `${layoutViewportHeight}px`);
    document.documentElement.dataset.desktopUiScale = String(scale);
  };

  const scheduleScale = () => {
    if (frame != null) window.cancelAnimationFrame(frame);
    frame = window.requestAnimationFrame(applyScale);
  };

  window.addEventListener("pywebviewready", scheduleScale);
  window.addEventListener("resize", scheduleScale);
  scheduleScale();

  return () => {
    window.removeEventListener("pywebviewready", scheduleScale);
    window.removeEventListener("resize", scheduleScale);
    if (frame != null) window.cancelAnimationFrame(frame);
  };
};
