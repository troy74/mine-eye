import { Component, type ErrorInfo, type ReactNode } from "react";

type Props = { children: ReactNode; fallback?: ReactNode };

type State = { error: Error | null };

export class ViewportErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("3D viewport error:", error, info.componentStack);
  }

  render() {
    if (this.state.error) {
      return (
        this.props.fallback ?? (
          <div
            style={{
              padding: 16,
              color: "#f85149",
              fontSize: 14,
              lineHeight: 1.5,
              maxWidth: 480,
            }}
          >
            <strong>3D view crashed.</strong> The rest of the app still works.{" "}
            <button
              type="button"
              onClick={() => this.setState({ error: null })}
              style={{ marginLeft: 8 }}
            >
              Retry
            </button>
            <pre
              style={{
                marginTop: 12,
                fontSize: 12,
                opacity: 0.9,
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
              }}
            >
              {this.state.error.message}
            </pre>
          </div>
        )
      );
    }
    return this.props.children;
  }
}
