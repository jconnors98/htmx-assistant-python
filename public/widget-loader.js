/**
 * Chat Widget Loader
 * 
 * Embed this widget on any website using:
 * <script src="https://bcca.ai/flask/widget-loader.js" 
 *         data-mode="your-mode-id" 
 *         data-theme="#82002d"
 *         data-position="bottom-right"></script>
 */

(function() {
  'use strict';
  
  // Get the current script tag to read data attributes
  const widgetScript = document.currentScript || (function() {
    const scripts = document.getElementsByTagName('script');
    return scripts[scripts.length - 1];
  })();
  
  // Configuration from data attributes
  const config = {
    mode: widgetScript.getAttribute('data-mode') || '',
    theme: widgetScript.getAttribute('data-theme') || '#82002d',
    position: widgetScript.getAttribute('data-position') || 'bottom-right', // bottom-right, bottom-left, top-right, top-left
    baseUrl: widgetScript.getAttribute('data-base-url') || 'https://bcca.ai/flask',
    userToken: widgetScript.getAttribute('data-user-token') || ''
  };

  // Detect mobile device (not "small iframe"). Prefer UA-CH when available.
  const isMobileDevice = (() => {
    try {
      if (navigator.userAgentData && typeof navigator.userAgentData.mobile === 'boolean') {
        return navigator.userAgentData.mobile;
      }
    } catch (e) { /* ignore */ }
    const ua = (navigator.userAgent || '').toLowerCase();
    return /mobi|android|iphone|ipod|ipad/.test(ua);
  })();
  
  // Validate required config
  if (!config.mode) {
    console.error('Chat Widget Error: data-mode attribute is required');
    return;
  }
  
  // Position styles
  const positions = {
    'bottom-right': 'bottom: 20px; right: 20px;',
    'bottom-left': 'bottom: 20px; left: 20px;',
    'top-right': 'top: 20px; right: 20px;',
    'top-left': 'top: 20px; left: 20px;'
  };
  
  const positionStyle = positions[config.position] || positions['bottom-right'];
  const widgetOrigin = (() => {
    try {
      return new URL(config.baseUrl).origin;
    } catch (err) {
      console.warn('Chat Widget: unable to parse baseUrl origin', err);
      return null;
    }
  })();
  
  // Create iframe container
  const createWidget = function() {
    // Create container div
    const container = document.createElement('div');
    container.id = 'chat-widget-container';
    
    // Track desired size reported by iframe (defaults to compact footprint)
    // NOTE: keep this small so we don't block the page before the iframe reports its real size
    let desiredWidth = 72;
    let desiredHeight = 72;
    let isFullscreen = false;
    let isFabVisible = true;
    
    // Create iframe
    const iframe = document.createElement('iframe');
    const tokenQuery = config.userToken
      ? `&user_token=${encodeURIComponent(config.userToken)}`
      : '';
    const widgetUrl = `${config.baseUrl}/chat-widget.html?mode=${encodeURIComponent(config.mode)}&theme=${encodeURIComponent(config.theme)}&parent_mobile=${isMobileDevice ? '1' : '0'}${tokenQuery}`;
    
    iframe.src = widgetUrl;
    iframe.style.cssText = `
      border: none;
      border-radius: 24px;
      box-shadow: none;
      background: transparent;
    `;
    iframe.allow = 'clipboard-write';
    iframe.title = 'Chat Widget';
    // Ensure the wrapper doesn't eat clicks; only the iframe should be interactive.
    container.style.pointerEvents = 'none';
    iframe.style.pointerEvents = 'auto';
    
    // Helper to clamp and apply the container size without occupying extra space
    const applyContainerStyles = function() {
      if (container.dataset.hidden === 'true') {
        container.style.cssText = 'display: none;';
        iframe.style.display = 'none';
        return;
      }
      if (isFullscreen || (window.innerWidth <= 480 && !isFabVisible)) {
      container.style.cssText = `
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        top: 0;
        z-index: 999999;
        width: 100%;
        height: 100%;
        max-width: 100%;
        max-height: 100%;
        pointer-events: none;
      `;
        iframe.style.width = '100%';
        iframe.style.height = '100%';
        iframe.style.borderRadius = '0';
        return;
      }
      
      const clampedWidth = Math.max(380, Math.min(desiredWidth, window.innerWidth - 40));
      const clampedHeight = Math.max(80, Math.min(desiredHeight, window.innerHeight - 100));
      
      container.style.cssText = `
        position: fixed;
        ${positionStyle}
        z-index: 999999;
        /* Let the iframe define size so we don't reserve/block extra empty area */
        width: ${clampedWidth}px;
        height: ${clampedHeight}px;
        pointer-events: none;
      `;
      iframe.style.width = '100%';
      iframe.style.height = '100%';
      iframe.style.borderRadius = '24px';
    };
    
    // Responsive handling for mobile
    const handleResize = function() {
      applyContainerStyles();
    };
    
    // Listen for size messages from the iframe so the container only matches visible content
    const handleWidgetMessage = function(event) {
      const data = event.data || {};
      if (!data || data.source !== 'chat-widget' || data.type !== 'SIZE') return;
      if (widgetOrigin && event.origin !== widgetOrigin) return;

      if (data.hidden === true) {
        container.dataset.hidden = 'true';
        applyContainerStyles();
        return;
      } else {
        container.dataset.hidden = 'false';
      }
      
      const reportedWidth = (typeof data.width === 'number' && !Number.isNaN(data.width))
        ? data.width
        : null;
      const reportedHeight = (typeof data.height === 'number' && !Number.isNaN(data.height))
        ? data.height
        : null;
      
      if (typeof data.fullscreen === 'boolean') {
        isFullscreen = data.fullscreen;
      }
      if (typeof data.fab === 'boolean') {
        isFabVisible = data.fab;
      }

      if (reportedWidth !== null) {
        desiredWidth = reportedWidth;
      }
      
      // If the widget reports that it's expanded, ensure we request at least
      // the full expanded height (600px) even if the measured height is missing
      // or very small. This prevents the container from staying at the compact
      // 80px size when users send a message to auto-expand.
      if (data.expanded) {
        desiredHeight = Math.max(reportedHeight || 0, 600);
      } else if (reportedHeight !== null) {
        desiredHeight = reportedHeight;
      }
      
      applyContainerStyles();
    };
    
    window.addEventListener('resize', handleResize);
    window.addEventListener('message', handleWidgetMessage);
    handleResize();
    
    container.appendChild(iframe);
    document.body.appendChild(container);
    
    console.log('Chat Widget v0.22 loaded successfully');
  };

  // On mobile, try to pre-check mode settings to avoid any iframe flash.
  // If the fetch fails (CORS/network), we fall back to creating the widget and
  // let the iframe tell us to hide via postMessage.
  const maybeCreateWidget = function() {
    if (!isMobileDevice) {
      createWidget();
      return;
    }
    const modeUrl = `${config.baseUrl}/modes/${encodeURIComponent(config.mode)}`;
    fetch(modeUrl, { method: 'GET' })
      .then(res => res.ok ? res.json() : null)
      .then(data => {
        if (data && data.disable_widget_on_mobile) {
          // Do not create iframe/container at all.
          return;
        }
        createWidget();
      })
      .catch(() => {
        createWidget();
      });
  };
  
  // Wait for DOM to be ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', maybeCreateWidget);
  } else {
    maybeCreateWidget();
  }
  
})();

