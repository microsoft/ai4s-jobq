  document.addEventListener("DOMContentLoaded", function () {
    document.querySelectorAll('.wy-menu-vertical p.caption').forEach(el => {
      el.setAttribute('aria-hidden', 'true');
      el.setAttribute('role', 'presentation');
      el.innerHTML = ''; // Clear existing content

      const hr = document.createElement('hr');
      hr.setAttribute('aria-hidden', 'true');
      el.appendChild(hr);

    });
  });

