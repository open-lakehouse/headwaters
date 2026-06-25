import type { Meta, StoryObj } from "@storybook/react";
import { withFakeClient } from "../testing/fake-client.js";
import { JobDetailPanel } from "./JobDetail.js";

const meta: Meta<typeof JobDetailPanel> = {
  title: "Detail/JobDetailPanel",
  component: JobDetailPanel,
  args: { namespace: "prod-warehouse", name: "etl.customers.hourly" },
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

type Story = StoryObj<typeof JobDetailPanel>;

export const Loaded: Story = { decorators: [withFakeClient("ok")] };
export const Loading: Story = { decorators: [withFakeClient("loading")] };
export const ErrorState: Story = { decorators: [withFakeClient("error")] };
