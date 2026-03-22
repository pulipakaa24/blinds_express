/* ============================================================
   BlindMaster Landing Page — main.js
   - Time-based accent color (mirrors Flutter app theming)
   - Nav scroll behavior
   - Scroll reveal animations
   - Hero blind animation
   - Interactive demo slider
   - Early access form
   ============================================================ */

(function () {
  'use strict';

  // ----------------------------------------------------------
  // 1. Time-based theme (matches Flutter app color system)
  //    Orange 5–10am | Blue 10am–6pm | Purple 6pm+
  // ----------------------------------------------------------
  function getThemeForHour(hour) {
    if (hour >= 5 && hour < 10) {
      return {
        name: 'morning',
        accent: '#F97316',
        accentRgb: '249, 115, 22',
        accentDark: '#C2410C',
        accentGlow: 'rgba(249, 115, 22, 0.25)',
      };
    } else if (hour >= 10 && hour < 18) {
      return {
        name: 'day',
        accent: '#3B82F6',
        accentRgb: '59, 130, 246',
        accentDark: '#1D4ED8',
        accentGlow: 'rgba(59, 130, 246, 0.25)',
      };
    } else {
      return {
        name: 'evening',
        accent: '#7C3AED',
        accentRgb: '124, 58, 237',
        accentDark: '#5B21B6',
        accentGlow: 'rgba(124, 58, 237, 0.25)',
      };
    }
  }

  function applyTheme(theme) {
    const root = document.documentElement;
    root.style.setProperty('--accent', theme.accent);
    root.style.setProperty('--accent-rgb', theme.accentRgb);
    root.style.setProperty('--accent-dark', theme.accentDark);
    root.style.setProperty('--accent-glow', theme.accentGlow);
  }

  const hour = new Date().getHours();
  applyTheme(getThemeForHour(hour));

  // ----------------------------------------------------------
  // 2. Nav — scrolled state
  // ----------------------------------------------------------
  const nav = document.getElementById('nav');
  function updateNav() {
    if (window.scrollY > 40) {
      nav.classList.add('scrolled');
    } else {
      nav.classList.remove('scrolled');
    }
  }
  window.addEventListener('scroll', updateNav, { passive: true });
  updateNav();

  // ----------------------------------------------------------
  // 3. Mobile nav toggle
  // ----------------------------------------------------------
  const hamburger = document.getElementById('hamburger');
  const mobileNav = document.getElementById('mobileNav');
  let mobileOpen = false;

  hamburger.addEventListener('click', () => {
    mobileOpen = !mobileOpen;
    mobileNav.classList.toggle('open', mobileOpen);
  });

  document.querySelectorAll('.mobile-nav-link').forEach(link => {
    link.addEventListener('click', () => {
      mobileOpen = false;
      mobileNav.classList.remove('open');
    });
  });

  // ----------------------------------------------------------
  // 4. Scroll reveal
  // ----------------------------------------------------------
  const revealEls = document.querySelectorAll('.reveal');
  const revealObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          entry.target.classList.add('visible');
          revealObserver.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.12, rootMargin: '0px 0px -40px 0px' }
  );
  revealEls.forEach(el => revealObserver.observe(el));

  // ----------------------------------------------------------
  // 5. Hero blind animation
  //    Cycle through positions to demo the 0–10 position model
  // ----------------------------------------------------------
  const slats = document.querySelectorAll('.blind-slat');
  const sliderThumb = document.getElementById('sliderThumb');

  // Position 0–10: map to slat rotation angle
  // 0 = fully closed (slats flat, blocking light)
  // 5 = fully open (slats perpendicular, maximum light)
  // 10 = closed from opposite direction
  function positionToAngle(pos) {
    if (pos <= 5) {
      return (pos / 5) * 75; // 0° → 75°
    } else {
      return 75 - ((pos - 5) / 5) * 75; // 75° → 0°
    }
  }

  function positionToSliderPercent(pos) {
    return (pos / 10) * 100;
  }

  function setBlindPosition(pos) {
    const angle = positionToAngle(pos);
    slats.forEach(slat => {
      slat.style.transform = `rotateX(${angle}deg)`;
    });
    sliderThumb.style.left = `${positionToSliderPercent(pos)}%`;
  }

  // Animate through positions: 0 → 5 → 10 → 5 → 0, looping
  const demoPositions = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
  let demoIndex = 0;

  setBlindPosition(demoPositions[0]);

  setInterval(() => {
    demoIndex = (demoIndex + 1) % demoPositions.length;
    setBlindPosition(demoPositions[demoIndex]);
  }, 700);

  // ----------------------------------------------------------
  // 6. Interactive slider on demo window (drag/click)
  // ----------------------------------------------------------
  const sliderTrack = document.querySelector('.app-slider-track');

  function handleSliderInteraction(clientX) {
    const rect = sliderTrack.getBoundingClientRect();
    const pct = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
    const pos = Math.round(pct * 10);
    setBlindPosition(pos);
  }

  sliderTrack.addEventListener('mousedown', (e) => {
    handleSliderInteraction(e.clientX);
    const onMove = (e) => handleSliderInteraction(e.clientX);
    const onUp = () => {
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  });

  sliderTrack.addEventListener('touchstart', (e) => {
    handleSliderInteraction(e.touches[0].clientX);
    const onMove = (e) => handleSliderInteraction(e.touches[0].clientX);
    const onEnd = () => {
      window.removeEventListener('touchmove', onMove);
      window.removeEventListener('touchend', onEnd);
    };
    window.addEventListener('touchmove', onMove);
    window.addEventListener('touchend', onEnd);
  }, { passive: true });

  // ----------------------------------------------------------
  // 7. Early access form
  // ----------------------------------------------------------
  const form = document.getElementById('earlyAccessForm');
  const emailInput = document.getElementById('emailInput');

  form.addEventListener('submit', (e) => {
    e.preventDefault();
    const email = emailInput.value.trim();
    if (!email) return;

    // Replace form with success message
    const successMsg = document.createElement('p');
    successMsg.className = 'cs-success';
    successMsg.textContent = `You're on the list! We'll notify ${email} at launch.`;
    form.replaceWith(successMsg);
  });

})();
