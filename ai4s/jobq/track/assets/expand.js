// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Expand-to-fullscreen buttons for all graph containers.
// Dash does not execute inline <script> tags in the component tree (React limitation),
// so this must live in the assets/ folder to be auto-loaded.

(function () {
  // Inject CSS
  var style = document.createElement("style");
  style.textContent = [
    ".dash-graph { position: relative !important; overflow: visible !important; }",
    ".expand-btn {",
    "  position: absolute; top: 8px; right: 8px; z-index: 1000;",
    "  background: rgba(255,255,255,0.92); border: 1px solid #bbb;",
    "  border-radius: 4px; cursor: pointer; font-size: 16px;",
    "  line-height: 1; padding: 4px 8px; color: #555;",
    "  box-shadow: 0 1px 3px rgba(0,0,0,0.12);",
    "}",
    ".expand-btn:hover { background: #fff; color: #111; border-color: #888; }",
    ".graph-expanded {",
    "  position: fixed !important; top: 0 !important; left: 0 !important;",
    "  width: 100vw !important; height: 100vh !important;",
    "  z-index: 10000 !important; background: white !important;",
    "  margin: 0 !important; padding: 10px !important;",
    "  aspect-ratio: unset !important; grid-column: unset !important;",
    "}",
    ".graph-expanded .js-plotly-plot,",
    ".graph-expanded .plot-container,",
    ".graph-expanded .plotly {",
    "  width: 100% !important; height: 100% !important;",
    "}",
  ].join("\n");
  document.head.appendChild(style);

  // Add expand buttons to graph containers
  function addExpandButtons() {
    document.querySelectorAll(".dash-graph").forEach(function (container) {
      if (container.querySelector(".expand-btn")) return;
      var btn = document.createElement("button");
      btn.className = "expand-btn";
      btn.innerHTML = "\u26F6";
      btn.title = "Expand (Escape to close)";
      btn.addEventListener("click", function (e) {
        e.stopPropagation();
        container.classList.toggle("graph-expanded");
        btn.innerHTML = container.classList.contains("graph-expanded")
          ? "\u2715"
          : "\u26F6";
        setTimeout(function () {
          window.dispatchEvent(new Event("resize"));
        }, 50);
      });
      container.appendChild(btn);
    });
  }

  // Run after initial render and on DOM changes
  setTimeout(addExpandButtons, 2000);
  setTimeout(addExpandButtons, 5000);
  new MutationObserver(addExpandButtons).observe(document.body, {
    childList: true,
    subtree: true,
  });

  // Escape key closes expanded graph
  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape") {
      var expanded = document.querySelector(".graph-expanded");
      if (expanded) {
        expanded.classList.remove("graph-expanded");
        var btn = expanded.querySelector(".expand-btn");
        if (btn) btn.innerHTML = "\u26F6";
        setTimeout(function () {
          window.dispatchEvent(new Event("resize"));
        }, 50);
      }
    }
  });
})();
