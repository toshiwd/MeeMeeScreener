import { act } from "react";
import type { ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";

export type RenderClientHandle = {
  container: HTMLDivElement;
  root: Root;
  html: () => string;
  cleanup: () => void;
};

export async function renderClient(node: ReactNode): Promise<RenderClientHandle> {
  const container = document.createElement("div");
  document.body.appendChild(container);
  const root = createRoot(container);

  await act(async () => {
    root.render(node);
    await Promise.resolve();
  });

  let cleaned = false;
  const cleanup = () => {
    if (cleaned) return;
    cleaned = true;
    act(() => {
      root.unmount();
    });
    container.remove();
  };

  return {
    container,
    root,
    html: () => container.innerHTML,
    cleanup
  };
}
