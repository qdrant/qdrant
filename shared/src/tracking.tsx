import { sendSuccess } from "./tracking-callbacks";

/*
 * For this task, you'll be implementing a tracking system that logs events to the console.
 *
 * The `trackEvent` function should accept an object with two properties:
 * `type` - a string that can be either "navigation" or "click"
 * `data` - an object with any data that should be logged
 * `timestamp` - an optional number that represents the time the event was created
 * The function returns a promise that resolves after 1 second (representing an API call)
 * After the promise resolves, the `sendSuccess` function should be called with the `type`, `data`, and `timestamp` properties
 * We hope these are self-explanatory, but we're happy to answer any questions you might have!
 *
 * The user story is:
 *
 * 1. As a user, I want to track events so that I can analyze user behavior.
 *
 * 2. Before sending any events to the server, user consent should be granted by the user
 * by clicking on "Accept", and otherwise I want to queue them up and send them all at once
 * when the user grants consent.
 *
 * 3. Optional: I want to deduplicate events that are sent within 500ms of each other (to prevent RAGE clicks).
 */

type TrackEventProps = {
  type: "navigation" | "click";
  data: Record<string, unknown>;
};

type TrackEventTimestampProps = TrackEventProps & { timestamp: number };

/*
 * We're going to use this function below, and all available types defined here. The types and function signature should not be changed.
 */

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const trackEvent = (props: TrackEventTimestampProps) => {
  return new Promise<void>((resolve) => {
    setTimeout(() => {
      resolve();
    }, 1000);
  }).then(() => {
    sendSuccess(props);
  });
};

// eslint-disable-next-line @typescript-eslint/no-unused-vars
type Tracking = {
  trackEvent: (_: TrackEventProps) => void | Promise<void>;
  onConsent: () => void;
};
