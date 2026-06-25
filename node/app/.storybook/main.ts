import type { StorybookConfig } from "@storybook/react-vite";

// Stories live alongside the reusable components in lineage-ui (the package
// Storybook documents) plus any app-level stories.
const config: StorybookConfig = {
  stories: [
    "../../lineage-ui/src/**/*.stories.@(ts|tsx)",
    "../src/**/*.stories.@(ts|tsx)",
  ],
  addons: [],
  framework: { name: "@storybook/react-vite", options: {} },
};

export default config;
