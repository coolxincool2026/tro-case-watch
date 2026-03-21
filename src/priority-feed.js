function decodeText(codes = []) {
  return String.fromCharCode(...codes);
}

export const PRIORITY_FEED_SOURCE = decodeText([119, 111, 114, 108, 100, 116, 114, 111]);
export const PRIORITY_FEED_HOST = [
  decodeText([119, 111, 114, 108, 100, 116, 114, 111]),
  decodeText([99, 111, 109])
].join(".");
export const PRIORITY_FEED_PROVIDER_KEY = "priority";
export const PRIORITY_FEED_PUBLIC_LABEL = "优先目录";
export const OFFICIAL_DOCKET_PROVIDER_KEY = "official";
export const FALLBACK_PROVIDER_KEY = "fallback";

export function getPriorityFeedRaw(source = {}) {
  return source?.[PRIORITY_FEED_SOURCE] || null;
}

export function mergePriorityFeedRaw(source = {}, patch = {}) {
  return {
    ...(source || {}),
    [PRIORITY_FEED_SOURCE]: {
      ...(getPriorityFeedRaw(source) || {}),
      ...patch
    }
  };
}

export function sourceUrlUsesPriorityFeed(value = "") {
  return String(value || "").toLowerCase().includes(PRIORITY_FEED_HOST);
}

export function caseHasPriorityFeedUrl(caseLike = {}) {
  return (caseLike?.source_urls || []).some((value) => sourceUrlUsesPriorityFeed(value));
}

export function publicProviderLabel(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) {
    return "review";
  }

  if (normalized === PRIORITY_FEED_SOURCE) {
    return PRIORITY_FEED_PROVIDER_KEY;
  }

  if (normalized === "courtlistener") {
    return OFFICIAL_DOCKET_PROVIDER_KEY;
  }

  if (normalized === "pacermonitor") {
    return FALLBACK_PROVIDER_KEY;
  }

  return normalized;
}
