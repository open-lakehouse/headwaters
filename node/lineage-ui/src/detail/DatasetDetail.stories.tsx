import type { Meta, StoryObj } from "@storybook/react";
import { withFakeClient } from "../testing/fake-client.js";
import { DatasetDetailPanel } from "./DatasetDetail.js";

const meta: Meta<typeof DatasetDetailPanel> = {
  title: "Detail/DatasetDetailPanel",
  component: DatasetDetailPanel,
  args: { namespace: "warehouse", name: "silver.customers" },
  decorators: [
    (Story) => (
      <div
        style={{
          width: 384,
          borderLeft: "1px solid var(--border)",
          height: "100vh",
        }}
      >
        <Story />
      </div>
    ),
  ],
};
export default meta;

type Story = StoryObj<typeof DatasetDetailPanel>;

export const Loaded: Story = { decorators: [withFakeClient("ok")] };
export const Loading: Story = { decorators: [withFakeClient("loading")] };
export const ErrorState: Story = { decorators: [withFakeClient("error")] };
