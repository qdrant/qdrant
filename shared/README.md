// TODO: implement onConsent to set a state in the app to remember the user's choice
// 1. Then use this state to decide whether to track events or not.
// 2. Then provide a trackEvent function that stores the tracked events in a queue, and flushes them when the user gives consent.
// 3. Extra: deduplicate equal events that occur within 500ms (throttle) - this will help prevent RAGE clicks.

# README

Welcome again. Let's share a few goals for this exercise:

- We'd like the solution to be idiomatic of React.
- We'd like TypeScript types (we're in strict-mode) to be correct.
- We'd like the solution to be reusable and maintenable.
- We'd like for the solution to be elegant and right to the point.

Your starting point is `src/tracking.tsx`, let's head over there and read some more...
