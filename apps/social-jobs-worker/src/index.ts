import { runLoop } from "./loop.js";

runLoop().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});
