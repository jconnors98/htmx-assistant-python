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
    baseUrl: widgetScript.getAttribute('data-base-url') || 'https://bcca.ai/flask'
  };
  
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
    
    // Create iframe
    const iframe = document.createElement('iframe');
    const widgetUrl = `${config.baseUrl}/chat-widget.html?mode=${encodeURIComponent(config.mode)}&theme=${encodeURIComponent(config.theme)}`;
    
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
      if (window.innerWidth <= 480) {
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
      
      const clampedWidth = Math.max(280, Math.min(desiredWidth, window.innerWidth - 40));
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
      
      if (typeof data.width === 'number' && !Number.isNaN(data.width)) {
        desiredWidth = data.width;
      }
      if (typeof data.height === 'number' && !Number.isNaN(data.height)) {
        desiredHeight = data.height;
      }
      
      applyContainerStyles();
    };
    
    window.addEventListener('resize', handleResize);
    window.addEventListener('message', handleWidgetMessage);
    handleResize();
    
    container.appendChild(iframe);
    document.body.appendChild(container);
    
    console.log('Chat Widget loaded successfully');
  };
  
  // Wait for DOM to be ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', createWidget);
  } else {
    createWidget();
  }
  
})();

