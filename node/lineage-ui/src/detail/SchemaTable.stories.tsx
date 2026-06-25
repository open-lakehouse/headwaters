import type { Meta, StoryObj } from "@storybook/react";
import { mockDataset } from "../testing/mock-data.js";
import { SchemaTable } from "./SchemaTable.js";

const meta: Meta<typeof SchemaTable> = {
  title: "Detail/SchemaTable",
  component: SchemaTable,
  decorators: [
    (Story) => (
      <div style={{ maxWidth: 600, padding: 24 }}>
        <Story />
      </div>
    ),
  ],
};
export default meta;

type Story = StoryObj<typeof SchemaTable>;

export const WithColumns: Story = {
  args: { fields: mockDataset.fields },
};

export const Empty: Story = {
  args: { fields: [] },
};
