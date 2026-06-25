import type { Meta, StoryObj } from "@storybook/react";
import { mockStatBuckets } from "../testing/mock-data.js";
import { StatsView } from "./StatsView.js";

const meta: Meta<typeof StatsView> = {
  title: "Home/StatsView",
  component: StatsView,
  decorators: [
    (Story) => (
      <div style={{ maxWidth: 480, padding: 24 }}>
        <Story />
      </div>
    ),
  ],
};
export default meta;

type Story = StoryObj<typeof StatsView>;

export const Populated: Story = {
  args: { title: "Lineage events", buckets: mockStatBuckets },
};

export const Sparse: Story = {
  args: { title: "Jobs", buckets: mockStatBuckets.slice(0, 5) },
};

export const Empty: Story = {
  args: { title: "Datasets", buckets: [] },
};
