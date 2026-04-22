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

  render() {
    const { error, info } = this.state;
    if (!error) {
      return this.props.children;
    }

    return (
      <div className="error-boundary">
        <h1>Something went wrong</h1>
        <p>{error.message}</p>
        <pre>{error.stack}</pre>
        {info?.componentStack && (
          <pre>{info.componentStack}</pre>
        )}
      </div>
    );
  }
}
