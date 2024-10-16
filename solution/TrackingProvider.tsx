import {
  createContext,
  ReactNode,
  useContext,
  useEffect,
  useState,
} from "react";
import {
  dedupSuccess,
  flushFailure,
  flushSuccess,
  queuedSuccess,
  sendSuccess,
} from "./tracking-callbacks";

type TrackEventProps = {
  type: "navigation" | "click";
  data: Record<string, unknown>;
};

const trackEvent = (props: TrackEventProps & { timestamp?: number }) => {
  return new Promise<void>((resolve) => {
    setTimeout(() => {
      resolve();
    }, 1000);
  }).then(() => {
    sendSuccess(props);
  });
};

const deduplicate = <TArgs extends unknown[], TReturn>(
  callback: (...args: TArgs) => TReturn,
  delay = 500
) => {
  const cache = new Map<string, number>();

  return (...args: TArgs) => {
    const key = JSON.stringify(args);
    const now = Date.now();

    if (cache.has(key)) {
      const lastNow = cache.get(key)!;
      console.log("[deduplicate] 🕒 Last event was", now - lastNow, "ms ago");
      if (delay > now - lastNow) {
        dedupSuccess(args);
        cache.set(key, now);
        return;
      }
      cache.delete(key);
    }

    cache.set(key, now);
    return callback(...args);
  };
};

type Tracking = {
  trackEvent: (_: TrackEventProps) => void | Promise<void>;
  onConsent: () => void;
};

const TrackingContext = createContext<Tracking>({
  trackEvent: () => undefined,
  onConsent: () => undefined,
});

export const TrackingProvider = ({ children }: { children: ReactNode }) => {
  const [hasConsent, setHasConsent] = useState(false);

  const [{ maybeTrackEvent, flush }] = useState(() => {
    const queue = new Set<TrackEventProps>();
    let didFlush = false;

    return {
      // flush: async () => {
      //   for (const props of queue) {
      //     await trackEvent(props);
      //   }
      //   didFlush = true;
      // },
      flush: async () => {
        const promises: Promise<void>[] = [];
        for (const props of queue) {
          promises.push(trackEvent(props));
        }
        await Promise.all(promises);
        didFlush = true;
      },
      maybeTrackEvent: deduplicate((props: TrackEventProps) => {
        const propsWithTimestamp = { ...props, timestamp: Date.now() };
        if (didFlush) {
          return trackEvent(propsWithTimestamp);
        }
        queue.add(propsWithTimestamp);
        queuedSuccess(props);
      }),
    };
  });

  useEffect(() => {
    if (hasConsent) {
      flush().then(flushSuccess).catch(flushFailure);
    }
  }, [flush, hasConsent]);

  return (
    <TrackingContext.Provider
      value={{
        trackEvent: maybeTrackEvent,
        onConsent() {
          setHasConsent(true);
        },
      }}
    >
      {children}
    </TrackingContext.Provider>
  );
};

export const useTracking = () => {
  return useContext(TrackingContext);
};
