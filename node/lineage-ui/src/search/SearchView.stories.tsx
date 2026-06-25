import type { Meta, StoryObj } from "@storybook/react";
import { useState } from "react";
import { withFakeClient } from "../testing/fake-client.js";
import { SearchView } from "./SearchView.js";

// SearchView is controlled (query lives in the host); wrap it so the input works
// in isolation.
function Harness({ initialQuery }: { initialQuery: string }) {
  const [q, setQ] = useState(initialQuery);
  return <SearchView query={q} onQueryChange={setQ} />;
}

const meta: Meta<typeof Harness> = {
  title: "Search/SearchView",
  component: Harness,
  decorators: [
    withFakeClient("ok"),
    (Story) => (
      <div style={{ height: "100vh" }}>
        <Story />
      </div>
    ),
  ],
};
export default meta;

type Story = StoryObj<typeof Harness>;

export const Results: Story = { args: { initialQuery: "customers" } };
export const Empty: Story = {
  args: { initialQuery: "no-such-thing" },
  decorators: [withFakeClient("empty")],
};
export const Initial: Story = { args: { initialQuery: "" } };
