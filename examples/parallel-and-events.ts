import { FlowForge } from "flowforge";

const app = new FlowForge({ concurrency: 8 });

app
  .create<{ orderId: string }>("process-order")
  .step("load-order", async ({ input }) => ({ id: input.orderId, total: 42 }))
  .parallel([
    {
      id: "reserve-stock",
      run: async ({ previous }) => ({ reservation: "stock-reserved", order: previous }),
    },
    {
      id: "authorize-payment",
      run: async ({ previous }) => ({ payment: "authorized", order: previous }),
    },
  ])
  .step("confirm", async ({ outputs }) => ({
    inventory: outputs["reserve-stock"],
    payment: outputs["authorize-payment"],
  }));

app.onEvent<{ orderId: string }>("orders:created", "process-order");

await app.emit("orders:created", { orderId: "order-123" });
await app.close();
