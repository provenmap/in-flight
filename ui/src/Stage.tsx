// Mounts a view that draws itself into a container, such as the 2D canvas, and keeps its flow and labels current.
import { Component, useEffect, useRef, type ReactNode, type RefObject } from 'react';
import type { Labels } from './labels';
import type { Model } from './model';
import type { Flow } from './protocol';
import type { SceneView } from './view2d';

export interface StageProps {
  model: Model;
  flow: Flow | null;
  labels: Labels;
  /** The mounted view, for the frame loop to draw and for `window.rif` to name. */
  viewRef: RefObject<SceneView | null>;
}

export function ClassStage({ make, model, flow, labels, viewRef }: StageProps & { make: (el: HTMLElement, model: Model) => SceneView }) {
  const el = useRef<HTMLDivElement>(null);

  // Effects run in order, so a new view gets the flow and labels from the two below in the same commit.
  useEffect(() => {
    const view = make(el.current!, model);
    viewRef.current = view;
    return () => {
      viewRef.current = null;
      view.dispose();
    };
  }, [make, model, viewRef]);

  useEffect(() => {
    if (flow) viewRef.current?.setFlow(flow);
  }, [flow, make, viewRef]);

  useEffect(() => {
    viewRef.current?.setLabels(labels);
  }, [labels, make, viewRef]);

  return <div ref={el} className="fixed inset-0 [&_canvas]:block" aria-label="The order-to-cash flow, live" />;
}

/** Falls back when the 3D view cannot load or render: no WebGL after all, or a failed chunk. */
export class Fallback extends Component<{ onError: (e: unknown) => void; children: ReactNode }, { failed: boolean }> {
  override state = { failed: false };

  static getDerivedStateFromError() {
    return { failed: true };
  }

  override componentDidCatch(e: unknown) {
    this.props.onError(e);
  }

  override render() {
    return this.state.failed ? null : this.props.children;
  }
}
