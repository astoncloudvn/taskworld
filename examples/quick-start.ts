import { FlowForge } from "flowforge";

interface SignupInput {
  username: string;
  email: string;
}

const app = new FlowForge({ concurrency: 5, retries: 3 });
const userStore = new Map<string, SignupInput & { validated: boolean }>();
const notificationOutbox: Array<{ userId: string; to: string; template: string }> = [];

app
  .create<SignupInput>("user-registration")
  .step("validate", async ({ input }) => {
    if (!input.username || !input.email.includes("@")) {
      throw new Error("A username and valid email are required.");
    }
    return { ...input, validated: true };
  })
  .step("save", ({ previous }) => {
    const userId = `user-${previous.username}`;
    userStore.set(userId, previous);
    return { userId, profile: previous };
  })
  .step("prepare-notification", ({ previous, log }) => {
    const message = {
      userId: previous.userId,
      to: previous.profile.email,
      template: "welcome",
    };
    notificationOutbox.push(message);
    log("Welcome message added to the example outbox.");
    return message;
  });

const result = await app.run<SignupInput>("user-registration", {
  input: { username: "green_alley", email: "hello@example.com" },
  metadata: { source: "website" },
});

if (result.status !== "succeeded") {
  throw new Error(result.error?.message ?? `Workflow ended as ${result.status}.`);
}

console.log(result.output);
await app.close();
