import type { Decorator, Preview } from "@storybook/react";
import { useEffect } from "react";
// The app's globals.css pulls in Tailwind + the @source directive for
// lineage-ui, plus the ReactFlow stylesheet — exactly what the components need.
import "../src/globals.css";

// Reflect the toolbar theme onto <html class="dark">, the same hook the app's
// ThemeProvider drives, so Tailwind dark: variants apply in stories. Lets every
// lineage-ui component be reviewed in both themes without a backend.
function ThemeSync({ theme }: { theme: string }) {
  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark");
  }, [theme]);
  return null;
}

const withTheme: Decorator = (Story, context) => {
  const theme = context.globals.theme ?? "light";
  return (
    <>
      <ThemeSync theme={theme} />
      <div className="bg-background text-foreground">
        <Story />
      </div>
    </>
  );
};

const preview: Preview = {
  decorators: [withTheme],
  parameters: {
    layout: "fullscreen",
    controls: { expanded: true },
  },
  globalTypes: {
    theme: {
      description: "Color theme",
      defaultValue: "light",
      toolbar: {
        title: "Theme",
        icon: "circlehollow",
        items: [
          { value: "light", icon: "sun", title: "Light" },
          { value: "dark", icon: "moon", title: "Dark" },
        ],
        dynamicTitle: true,
      },
    },
  },
};

export default preview;
