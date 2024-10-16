const dateFormatter = new Intl.DateTimeFormat("en-US", {
  dateStyle: "short",
  timeStyle: "short",
});

export const sendSuccess = ({
  type,
  data,
  timestamp,
}: {
  type: string;
  data: unknown;
  timestamp?: number;
}) => {
  console.log(
    `[trackEvent] 🚀 Sending '${type}' event with data:`,
    data,
    timestamp ? ` (${dateFormatter.format(new Date(timestamp))})` : "",
  );
};

export const dedupSuccess = (args: unknown[]) => {
  console.log("[deduplicate] 🚫 Ignoring duplicate event", args);
};

export const queuedSuccess = (props: { type: string; data: unknown }) => {
  console.log("[trackEvent] 🚦 Queued event", props);
};

export const flushSuccess = () => {
  console.log("[trackEvent] 🎉 Flushed all events");
};

export const flushFailure = (error: unknown) => {
  console.log("[trackEvent] 🎉 Error flushing events", error);
};
