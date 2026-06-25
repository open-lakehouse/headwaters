import type { Meta, StoryObj } from "@storybook/react";
import { withFakeClient } from "../testing/fake-client.js";
import { HomeView } from "./HomeView.js";

const meta: Meta<typeof HomeView> = {
  title: "Home/HomeView",
  component: HomeView,
  decorators: [withFakeClient("ok")],
};
export default meta;

type Story = StoryObj<typeof HomeView>;

export const Default: Story = {};
export const Empty: Story = { decorators: [withFakeClient("empty")] };
