import { Monitor, Moon, Sun } from "lucide-react";
import { useTheme } from "./ThemeProvider.js";
import { Button } from "./ui/button.js";

const ORDER = ["light", "dark", "system"] as const;

const META = {
  light: { icon: Sun, label: "Light" },
  dark: { icon: Moon, label: "Dark" },
  system: { icon: Monitor, label: "System" },
} as const;

export function ThemeToggle() {
  const { theme, setTheme } = useTheme();
  const { icon: Icon, label } = META[theme];

  function cycle() {
    const next = ORDER[(ORDER.indexOf(theme) + 1) % ORDER.length];
    setTheme(next);
  }

  return (
    <Button
      variant="ghost"
      size="icon"
      onClick={cycle}
      aria-label={`Theme: ${label} (click to change)`}
      title={`Theme: ${label}`}
    >
      <Icon className="h-4 w-4" />
    </Button>
  );
}
