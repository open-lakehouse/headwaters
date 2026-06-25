import type { Preview } from "@storybook/react";
// The app's globals.css pulls in Tailwind + the @source directive for
// lineage-ui, plus the ReactFlow stylesheet — exactly what the components need.
import "../src/globals.css";

const preview: Preview = {
  parameters: {
    layout: "fullscreen",
    controls: { expanded: true },
  },
};

export default preview;
