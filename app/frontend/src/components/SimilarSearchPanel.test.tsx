// @vitest-environment jsdom
import { act } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderClient, type RenderClientHandle } from "../test/renderClient";
import SimilarSearchPanel from "./SimilarSearchPanel";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const mocks = vi.hoisted(() => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  prefetchDetailChartFrames: vi.fn(),
}));

vi.mock("../api", () => ({
  api: {
    get: mocks.apiGet,
    post: mocks.apiPost,
  },
}));

vi.mock("../routes/detail/detailChartPrefetch", () => ({
  prefetchDetailChartFrames: mocks.prefetchDetailChartFrames,
}));

const renderPanel = async (): Promise<RenderClientHandle> =>
  renderClient(
    <MemoryRouter>
      <SimilarSearchPanel
        isOpen={true}
        onClose={() => undefined}
        queryTicker="2531"
        queryAsOf="2026-05-28"
      />
    </MemoryRouter>
  );

const flush = async () => {
  await Promise.resolve();
  await Promise.resolve();
};

describe("SimilarSearchPanel", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mocks.apiGet.mockReset();
    mocks.apiPost.mockReset();
    mocks.prefetchDetailChartFrames.mockReset();
    mocks.apiGet.mockImplementation((url: string) => {
      if (url === "/search/similar/status") {
        return Promise.resolve({ data: { status: { running: false, finished_at: null } } });
      }
      if (url === "/search/similar") {
        return Promise.resolve({ data: [] });
      }
      return Promise.resolve({ data: {} });
    });
  });

  afterEach(() => {
    document.body.innerHTML = "";
    vi.useRealTimers();
  });

  it("uses Japanese labels and a longer timeout for similarity search", async () => {
    const render = await renderPanel();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
      await flush();
    });

    expect(render.container.textContent).toContain("類似チャート検索");
    expect(render.container.textContent).toContain("対象: 2531");
    expect(render.container.textContent).toContain("重視: 長期(60ヶ月) 0% / 短期(12ヶ月) 100%");
    expect(render.container.textContent).toContain("類似したチャートは見つかりませんでした。");

    const searchCall = mocks.apiGet.mock.calls.find(([url]) => url === "/search/similar");
    expect(searchCall?.[1]).toMatchObject({
      params: {
        ticker: "2531",
        asof: "2026-05-28",
        alpha: 0,
        k: 30,
        include_vectors: true,
      },
      timeout: 60000,
    });

    render.cleanup();
  });

  it("shows a user-facing timeout message instead of the raw axios timeout", async () => {
    mocks.apiGet.mockImplementation((url: string) => {
      if (url === "/search/similar/status") {
        return Promise.resolve({ data: { status: { running: false, finished_at: null } } });
      }
      if (url === "/search/similar") {
        const error = new Error("timeout of 15000ms exceeded") as Error & { code?: string };
        error.code = "ECONNABORTED";
        return Promise.reject(error);
      }
      return Promise.resolve({ data: {} });
    });

    const render = await renderPanel();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
      await flush();
    });

    expect(render.container.textContent).toContain("検索に時間がかかっています。少し待ってから更新してください。");
    expect(render.container.textContent).not.toContain("timeout of 15000ms exceeded");

    render.cleanup();
  });
});
