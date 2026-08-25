import { Show, Suspense, lazy } from "solid-js"
import type { SessionPageProps } from "./session-page-types"
import { SessionGraphDrawer } from "./session-graph-drawer"
import { SessionSidePanelLoading } from "./panel-loading-state"
import type { createSessionSidePanelController } from "./session-side-panel-controller"

const SessionSidePanel = lazy(() =>
  import("./session-side-panel").then((module) => ({ default: module.SessionSidePanel })),
)

export function SessionPageSidePanel(props: {
  page: SessionPageProps
  sidePanel: ReturnType<typeof createSessionSidePanelController>
}) {
  return (
    <Show when={props.sidePanel.mounted() ? props.sidePanel.session() : undefined}>
      {(selected) => (
        <Suspense
          fallback={<SessionSidePanelLoading open={props.sidePanel.open()} widthRatio={props.sidePanel.widthRatio()} />}
        >
          <SessionSidePanel
            open={props.sidePanel.open()}
            widthRatio={props.sidePanel.widthRatio()}
            session={selected()}
            data={props.page.data}
            providers={props.page.providers}
            mcp={props.page.mcp}
            lsp={props.page.lsp}
            config={props.page.config}
            gui={props.page.gui}
            subscribeGlobalEvents={props.page.subscribeGlobalEvents}
            directory={props.page.sidePanelDirectory ?? selected().directory}
            graph={props.page.graph}
            graphSelectedNodeID={props.page.graphSelectedNodeID ?? ""}
            graphTopology={props.page.graphTopology}
            retryGraphTopology={props.page.retryGraphTopology}
            openGraphNode={props.page.openGraphNode}
            openGraphNodeFullPage={props.page.openGraphNodeFullPage}
            canOpenGraphNodeFullPage={props.page.canOpenGraphNodeFullPage}
            graphDrawer={
              props.sidePanel.centerCollapsed() && props.page.graphNodeSessionID ? (
                <SessionGraphDrawer page={props.page} />
              ) : undefined
            }
            approveGraphGate={(gate, approved) => props.page.approveGoalNode?.(gate.goalID, gate.nodeID, approved)}
            request={props.sidePanel.request()}
            startResize={props.sidePanel.startResize}
            toggleMaximized={props.sidePanel.toggleMaximized}
            resizeByKeyboard={props.sidePanel.resizeByKeyboard}
          />
        </Suspense>
      )}
    </Show>
  )
}
