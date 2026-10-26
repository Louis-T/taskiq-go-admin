// Small progressive enhancements; everything else is htmx + server rendering.
(function () {
  "use strict";

  // Time zone: the server renders times in the zone stored in the "tz"
  // cookie. Set it from the browser and reload once if it changed.
  try {
    var tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    if (tz && document.documentElement.dataset.tz !== tz) {
      document.cookie = "tz=" + encodeURIComponent(tz) + "; path=/; max-age=31536000; samesite=lax";
      if (sessionStorage.getItem("tz-reloaded") !== tz) {
        sessionStorage.setItem("tz-reloaded", tz);
        location.reload();
      }
    }
  } catch (e) {}

  // Live updates: the server pushes "changed" over SSE; live regions listen
  // for the "tasks-changed" event (see hx-trigger in the templates).
  window.liveRefreshAllowed = function () {
    return document.visibilityState === "visible" && !document.querySelector("dialog[open]");
  };
  function changed() {
    if (window.htmx) htmx.trigger(document.body, "tasks-changed");
  }
  if (window.EventSource) {
    var es = new EventSource("/events");
    var lost = false;
    es.addEventListener("changed", changed);
    es.onerror = function () { lost = true; };
    es.onopen = function () { if (lost) { lost = false; changed(); } }; // catch up after a reconnect
  }
  // Catch up when coming back to a hidden tab, or after closing a dialog.
  document.addEventListener("visibilitychange", function () {
    if (document.visibilityState === "visible") changed();
  });
  document.addEventListener("close", changed, true);

  function toast(msg) {
    var el = document.getElementById("toast");
    if (!el) return;
    el.textContent = msg;
    el.classList.add("show");
    clearTimeout(el._t);
    el._t = setTimeout(function () { el.classList.remove("show"); }, 1800);
  }

  document.addEventListener("click", function (ev) {
    var copy = ev.target.closest("[data-copy]");
    if (copy) {
      ev.preventDefault();
      navigator.clipboard.writeText(copy.dataset.copy).then(
        function () { toast("ID copied to clipboard"); },
        function () { toast("Could not copy"); }
      );
      return;
    }
    var opener = ev.target.closest("[data-dialog]");
    if (opener) {
      var dlg = document.getElementById(opener.dataset.dialog);
      if (dlg) dlg.showModal();
      return;
    }
    if (ev.target.closest("[data-theme-toggle]")) {
      var root = document.documentElement;
      var dark = root.dataset.theme
        ? root.dataset.theme === "dark"
        : matchMedia("(prefers-color-scheme: dark)").matches;
      root.dataset.theme = dark ? "light" : "dark";
      try { localStorage.setItem("theme", root.dataset.theme); } catch (e) {}
      return;
    }
    // Clickable rows: plain clicks on a row (not on its links, buttons or
    // dialogs) open data-href; ctrl/cmd-click opens a new tab.
    var row = ev.target.closest("tr[data-href]");
    if (row && !ev.target.closest("a, button, input, select, dialog, summary") && !window.getSelection().toString()) {
      if (ev.ctrlKey || ev.metaKey) window.open(row.dataset.href, "_blank");
      else location.href = row.dataset.href;
      return;
    }
    var back = ev.target.closest("[data-back]");
    if (back && history.length > 1 && document.referrer.indexOf(location.origin) === 0) {
      ev.preventDefault();
      history.back();
    }
  });

  // Middle-click on a row opens a new tab.
  document.addEventListener("auxclick", function (ev) {
    var row = ev.button === 1 && ev.target.closest("tr[data-href]");
    if (row && !ev.target.closest("a, button")) window.open(row.dataset.href, "_blank");
  });

  // Close a dialog when clicking its backdrop.
  document.addEventListener("click", function (ev) {
    if (ev.target.tagName !== "DIALOG") return;
    var r = ev.target.getBoundingClientRect();
    if (ev.clientX < r.left || ev.clientX > r.right || ev.clientY < r.top || ev.clientY > r.bottom) {
      ev.target.close();
    }
  });
})();
