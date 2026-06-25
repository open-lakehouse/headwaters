import type { Meta, StoryObj } from "@storybook/react";
import { mockColumnGraph, mockTableGraph } from "../testing/mock-data.js";
import { LineageCanvas } from "./LineageCanvas.js";

// LineageCanvas is purely presentational — it takes a graph and renders it, so
// stories feed it fixtures directly with no client/transport at all.
const meta: Meta<typeof LineageCanvas> = {
  title: "Graph/LineageCanvas",
  component: LineageCanvas,
  decorators: [
    (Story) => (
      <div style={{ width: "100%", height: "100vh" }}>
        <Story />
      </div>
    ),
  ],
};
export default meta;

type Story = StoryObj<typeof LineageCanvas>;

export const TableLineage: Story = {
  args: { graph: mockTableGraph },
};

export const ColumnLineage: Story = {
  args: { graph: mockColumnGraph },
};

export const Selected: Story = {
  args: {
    graph: mockTableGraph,
    selectedId: "job:prod-warehouse:etl.customers.hourly",
  },
};

export const Empty: Story = {
  args: { graph: { graph: [] } as never },
};

// A wider graph exercising the layered layout with more nodes/edges.
export const DeepGraph: Story = {
  args: {
    graph: {
      graph: Array.from({ length: 7 }, (_, i) => ({
        id: `dataset:ns:t${i}`,
        type: i % 2 === 0 ? "DATASET" : "JOB",
        data: {
          id: { namespace: "ns", name: `t${i}` },
          name: `t${i}`,
          namespace: "ns",
        },
        inEdges:
          i === 0
            ? []
            : [
                {
                  origin: `dataset:ns:t${i - 1}`,
                  destination: `dataset:ns:t${i}`,
                },
              ],
        outEdges:
          i === 6
            ? []
            : [
                {
                  origin: `dataset:ns:t${i}`,
                  destination: `dataset:ns:t${i + 1}`,
                },
              ],
      })),
    } as never,
  },
};
