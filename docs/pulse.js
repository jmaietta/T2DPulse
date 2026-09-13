// TEK2day Pulse homepage behaviour. Extracted verbatim from the page template so
// the browser can cache it across rebuilds; load order matches the original inline blocks.
// Install prompt for PWA
let deferredPrompt;
window.addEventListener('beforeinstallprompt', (e) => {
  e.preventDefault();
  deferredPrompt = e;
  const wrap = document.querySelector('.install');
  if (wrap) wrap.style.display = 'block';
});

document.getElementById('btnInstall')?.addEventListener('click', async () => {
  if (!deferredPrompt) return;
  deferredPrompt.prompt();
  await deferredPrompt.userChoice;
  deferredPrompt = null;
  document.querySelector('.install').style.display = 'none';
});

// Service worker registration
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('./sw.js', { scope: './' });
  });
}

// Header shadow on scroll
window.addEventListener('scroll', () => {
  const h = document.querySelector('header.site');
  if (!h) return;
  h.classList.toggle('shadow', window.scrollY > 8);
}, { passive: true });

(function() {
  const cards = document.querySelectorAll('#tek2day-pulse article');
  if (!cards.length) return;

  for (const card of cards) {
    const link = card.querySelector('h3 a');
    if (!link) continue;

    const originalUrl = link.href;
    let permalink = card.dataset.permalink || originalUrl;
    
    // Convert relative URLs to absolute
    if (permalink.startsWith('/')) {
      permalink = window.location.origin + permalink;
    }
    
    const title = link.textContent.trim();
    const brand = 'via TEK2day Pulse';
    const shareTitle = `${title} - ${brand}`;
    const encodedUrl = encodeURIComponent(permalink);
    const xShareUrl = `https://twitter.com/intent/tweet?url=${encodedUrl}&text=${encodeURIComponent(shareTitle)}`;

    // Create share toolbar
    const bar = document.createElement('div');
    bar.className = 'share';
    bar.innerHTML = `
      <a class="share-btn" href="${xShareUrl}" target="_blank" rel="noopener noreferrer">Share to X</a>
      <button class="share-btn copy-url" type="button" data-url="${permalink}">Copy Link</button>
    `;
    card.querySelector('.article-content').appendChild(bar);
  }

  // Copy URL handler with native share fallback
  document.addEventListener('click', async (e) => {
    const btn = e.target.closest('.copy-url');
    if (!btn) return;

    const url = btn.dataset.url;

    // Prefer native share on mobile
    if (navigator.share) {
      try {
        await navigator.share({ url });
        return;
      } catch (_) {}
    }

    // Fallback to clipboard
    try {
      await navigator.clipboard.writeText(url);
    } catch (err) {
      const ta = document.createElement('textarea');
      ta.value = url;
      ta.style.cssText = 'position:fixed;opacity:0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      ta.remove();
    }
    
    const originalText = btn.textContent;
    btn.textContent = 'Copied!';
    setTimeout(() => btn.textContent = originalText, 1500);
  });
})();

(function() {
  const btn = document.getElementById('back-to-top');
  if (!btn) return;

  // Show/hide based on scroll position
  window.addEventListener('scroll', () => {
    // Threshold: 300px
    if (window.scrollY > 300) {
      btn.classList.add('visible');
    } else {
      btn.classList.remove('visible');
    }
  }, { passive: true });

  // Scroll up smoothly on click
  btn.addEventListener('click', () => {
    window.scrollTo({
      top: 0,
      behavior: 'smooth'
    });
    // For accessibility, move focus back to top
    document.body.focus(); 
  });
})();

(function() {
  // Search filter. While the query is empty the hero (lead story + Brief)
  // shows and the lead's grid copy stays hidden; a query hides the hero and
  // filters the grid, lead included.
  const container = document.getElementById('tek2day-pulse');
  const form = document.querySelector('.hdr-actions .search');
  const input = document.getElementById('t2d-q');
  const itemsContainer = container && container.querySelector('.items');
  const resultCount = document.getElementById('result-count');
  if (!form || !input || !container || !itemsContainer) return;

  const cards = Array.from(itemsContainer.querySelectorAll('article[data-card]'));
  const totalStories = cards.length;
  const defaultNote = resultCount ? resultCount.textContent : '';

  function haystack(card) {
    return [card.dataset.title, card.dataset.summary, card.dataset.source]
      .join(' ').replace(/\s+/g, ' ').trim().toLowerCase();
  }
  const index = cards.map(card => ({ card, text: haystack(card) }));

  function tokenize(q) {
    return (q || '').toLowerCase().trim().split(/\s+/).filter(Boolean);
  }

  function applyFilter() {
    const tokens = tokenize(input.value);
    const filtered = tokens.length > 0;
    document.body.classList.toggle('is-filtered', filtered);

    let shown = 0;
    for (const { card, text } of index) {
      const match = tokens.every(t => text.includes(t));
      card.hidden = !match;
      if (match) shown++;
    }

    let empty = document.getElementById('search-empty');
    if (!empty) {
      empty = document.createElement('div');
      empty.id = 'search-empty';
      empty.style.cssText = 'display:none;padding:2rem;color:var(--muted);text-align:center;grid-column:1/-1;';
      empty.setAttribute('role', 'status');
      empty.textContent = 'No articles found matching your search.';
      itemsContainer.appendChild(empty);
    }
    empty.style.display = filtered && shown === 0 ? 'block' : 'none';

    if (resultCount) {
      resultCount.textContent = filtered
        ? `${shown} ${shown === 1 ? 'story' : 'stories'} of ${totalStories}`
        : defaultNote;
    }
  }

  form.addEventListener('submit', (e) => {
    e.preventDefault();
    applyFilter();
    if (window.innerWidth > 720) {
      container.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  });

  input.addEventListener('input', applyFilter, { passive: true });

  // Support legacy inbound links like /?q=... but keep the canonical URL clean.
  try {
    const u = new URL(window.location.href);
    const q0 = u.searchParams.get('q');

    if (u.searchParams.has('q')) {
      u.searchParams.delete('q');
      const clean =
        u.pathname +
        (u.searchParams.toString() ? `?${u.searchParams.toString()}` : '') +
        u.hash;
      history.replaceState({}, '', clean);
    }

    if (q0) {
      input.value = q0;
      applyFilter();
    }
  } catch (_) {}
})();

(function() {
  // Only run if embedded in iframe
  if (window.self === window.top) return;
  
  document.addEventListener('click', function(e) {
    const link = e.target.closest('h3 a');
    if (!link) return;
    
    e.preventDefault();
    e.stopPropagation();
    
    window.parent.postMessage({
      type: 'article-click',
      url: link.href
    }, 'https://dash.tek2dayholdings.com');
  }, true);
})();
