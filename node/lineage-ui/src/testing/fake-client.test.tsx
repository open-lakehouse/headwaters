// @vitest-environment happy-dom

// The Storybook decorator renders real components through the fake host.

import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { JobBrowser } from "../browser/JobBrowser.js";
import { withFakeClient } from "./fake-client.js";
import { mockJob } from "./mock-data.js";

afterEach(cleanup);

describe("withFakeClient", () => {
  it("serves the fixtures through the fake host", async () => {
    render(withFakeClient("ok")(() => <JobBrowser />));
    expect(await screen.findByText(mockJob.name)).toBeTruthy();
  });
});
