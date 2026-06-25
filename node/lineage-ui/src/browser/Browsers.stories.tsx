import type { Meta, StoryObj } from "@storybook/react";
import type { ReactNode } from "react";
import { withFakeClient } from "../testing/fake-client.js";
import { DatasetBrowser } from "./DatasetBrowser.js";
import { JobBrowser } from "./JobBrowser.js";

const frame = (Story: () => ReactNode) => (
  <div
    style={{
      width: 520,
      height: "100vh",
      borderRight: "1px solid var(--border)",
    }}
  >
    <Story />
  </div>
);

const meta: Meta<typeof DatasetBrowser> = {
  title: "Browser/DatasetBrowser",
  component: DatasetBrowser,
  decorators: [frame],
};
export default meta;

type Story = StoryObj<typeof DatasetBrowser>;

export const Datasets: Story = { decorators: [withFakeClient("ok")] };
export const DatasetsEmpty: Story = { decorators: [withFakeClient("empty")] };
export const DatasetsLoading: Story = {
  decorators: [withFakeClient("loading")],
};
export const DatasetsError: Story = { decorators: [withFakeClient("error")] };

export const Jobs: StoryObj<typeof JobBrowser> = {
  render: () => <JobBrowser />,
  decorators: [withFakeClient("ok")],
};
