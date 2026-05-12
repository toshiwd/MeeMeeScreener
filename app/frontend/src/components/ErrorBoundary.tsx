import React from "react";
import { reportFrontendFatalDiagnostics } from "../utils/fatalDiagnostics";

type ErrorBoundaryState = {
  error: Error | null;
  info: React.ErrorInfo | null;
};

type ErrorBoundaryProps = {
  children: React.ReactNode;
};

export default class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { error: null, info: null };
  }

  static getDerivedStateFromError(error: Error) {
    return { error, info: null };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    this.setState({ error, info });
    void reportFrontendFatalDiagnostics({
      source: "error-boundary",
      error,
      componentStack: info.componentStack,
    });
  }

  private buildDiagnosticsText() {
    const { error, info } = this.state;
    return [
      "MeeMee frontend fatal error",
      error?.message ? `message: ${error.message}` : null,
      error?.stack ? `stack: ${error.stack}` : null,
      info?.componentStack ? `componentStack: ${info.componentStack}` : null,
    ]
      .filter((line): line is string => Boolean(line))
      .join("\n\n");
  }

  private handleCopyDiagnostics = () => {
    const text = this.buildDiagnosticsText();
    if (!text || !navigator.clipboard?.writeText) return;
    void navigator.clipboard.writeText(text);
  };

  private handleReload = () => {
    window.location.reload();
  };

  render() {
    const { error } = this.state;
    if (!error) {
      return this.props.children;
    }

    return (
      <div className="error-boundary">
        <h1>画面の表示に問題が発生しました</h1>
        <p>再読み込みしても直らない場合は、診断情報をコピーして確認してください。</p>
        <div className="error-boundary-actions">
          <button type="button" onClick={this.handleReload}>再読み込み</button>
          <button type="button" onClick={this.handleCopyDiagnostics}>診断情報をコピー</button>
        </div>
      </div>
    );
  }
}
